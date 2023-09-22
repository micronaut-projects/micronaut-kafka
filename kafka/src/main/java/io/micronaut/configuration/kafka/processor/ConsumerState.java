/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.processor;

import io.micronaut.configuration.kafka.KafkaAcknowledgement;
import io.micronaut.configuration.kafka.KafkaMessage;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException;
import io.micronaut.configuration.kafka.seek.KafkaSeekOperations;
import io.micronaut.configuration.kafka.seek.KafkaSeeker;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.Argument;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * The internal state of the consumer.
 *
 * @author Denis Stepanov
 */
@Internal
final class ConsumerState {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerProcessor.class); // NOSONAR

    final ConsumerInfo info;
    final Consumer<?, ?> kafkaConsumer;
    final Set<String> subscriptions;
    Set<TopicPartition> assignments;

    private final KafkaConsumerProcessor kafkaConsumerProcessor;
    private final Object consumerBean;
    private Set<TopicPartition> pausedTopicPartitions;
    private Set<TopicPartition> pauseRequests;
    @Nullable
    private Map<Integer, PartitionRetryState> partitionRetries;
    private boolean autoPaused;
    private final Map<Argument<?>, Object> boundArguments;
    private boolean pollingStarted;
    private boolean failed;
    private volatile ConsumerCloseState closedState;

    ConsumerState(
        KafkaConsumerProcessor kafkaConsumerProcessor,
        ConsumerInfo info,
        Consumer<?, ?> consumer,
        Object consumerBean
    ) {
        this.kafkaConsumerProcessor = kafkaConsumerProcessor;
        this.info = info;
        this.kafkaConsumer = consumer;
        this.consumerBean = consumerBean;
        this.subscriptions = Collections.unmodifiableSet(kafkaConsumer.subscription());
        this.autoPaused = !info.autoStartup;
        this.boundArguments = new HashMap<>(2);
        Optional.ofNullable(info.consumerArg).ifPresent(argument -> boundArguments.put(argument, kafkaConsumer));
        this.closedState = ConsumerCloseState.NOT_STARTED;
    }

    synchronized void pause() {
        pause(assignments);
    }

    synchronized void pause(@NonNull Collection<TopicPartition> topicPartitions) {
        if (pauseRequests == null) {
            pauseRequests = new HashSet<>();
        }
        pauseRequests.addAll(topicPartitions);
    }

    synchronized void resume() {
        autoPaused = false;
        pauseRequests = null;
    }

    synchronized void resume(@NonNull Collection<TopicPartition> topicPartitions) {
        autoPaused = false;
        if (pauseRequests != null) {
            pauseRequests.removeAll(topicPartitions);
        }
    }

    synchronized boolean isPaused(@NonNull Collection<TopicPartition> topicPartitions) {
        if (pauseRequests == null || pausedTopicPartitions == null) {
            return false;
        }
        return pauseRequests.containsAll(topicPartitions) && pausedTopicPartitions.containsAll(topicPartitions);
    }

    synchronized void wakeUp() {
        kafkaConsumer.wakeup();
    }

    synchronized void close() {
        if (closedState == ConsumerCloseState.POLLING) {
            final Instant start = Instant.now();
            Instant silentTime = start;
            do {
                if (LOG.isTraceEnabled()) {
                    final Instant now = Instant.now();
                    if (now.isAfter(silentTime)) {
                        LOG.trace("Consumer {} is not closed yet (waiting {})", info.clientId, Duration.between(start, now));
                        // Inhibit TRACE messages for a while to avoid polluting the logs
                        silentTime = now.plusSeconds(5);
                    }
                }
            } while (closedState == ConsumerCloseState.POLLING);
        }
        LOG.debug("Consumer {} is closed", info.clientId);
    }

    synchronized void threadPollLoop() {
        try (kafkaConsumer) {
            //noinspection InfiniteLoopStatement
            while (true) { //NOSONAR
                refreshAssignmentsPollAndProcessRecords();
            }
        } catch (WakeupException e) {
            closedState = ConsumerCloseState.CLOSED;
        }
    }

    private void refreshAssignmentsPollAndProcessRecords() {
        refreshAssignments();
        try {
            pollAndProcessRecords();
        } catch (WakeupException e) {
            try {
                if (!failed && info.offsetStrategy != OffsetStrategy.DISABLED) {
                    kafkaConsumer.commitSync();
                }
            } catch (Exception ex) {
                LOG.warn("Error committing Kafka offsets on shutdown: {}", ex.getMessage(), ex);
            }
            throw e;
        } catch (Exception e) {
            handleException(e, null);
        }
    }

    private void refreshAssignments() {
        final Set<TopicPartition> newAssignments = kafkaConsumer.assignment();
        if (!newAssignments.equals(assignments)) {
            LOG.info("Consumer [{}] assignments changed: {} -> {}", info.clientId, assignments, newAssignments);
            assignments = Collections.unmodifiableSet(newAssignments);
        }
        if (autoPaused) {
            pause(assignments);
            kafkaConsumer.pause(assignments);
        }
    }

    private void pollAndProcessRecords() {
        failed = true;
        final ConsumerRecords<?, ?> consumerRecords = pollRecords();
        if (consumerRecords == null || consumerRecords.isEmpty()) {
            return; // No consumer records to process
        }
        if (info.isBatch) {
            processAsBatch(consumerRecords);
        } else {
            processIndividually(consumerRecords);
        }
        if (failed) {
            return;
        }
        if (info.offsetStrategy == OffsetStrategy.SYNC) {
            try {
                kafkaConsumer.commitSync();
            } catch (CommitFailedException e) {
                handleException(e, null);
            }
        } else if (info.offsetStrategy == OffsetStrategy.ASYNC) {
            kafkaConsumer.commitAsync(resolveCommitCallback());
        }
    }

    private ConsumerRecords<?, ?> pollRecords() {
        pauseTopicPartitions();
        final ConsumerRecords<?, ?> consumerRecords = poll();
        closedState = ConsumerCloseState.POLLING;
        if (!pollingStarted) {
            pollingStarted = true;
            kafkaConsumerProcessor.publishStartedPollingEvent(kafkaConsumer);
        }
        resumeTopicPartitions();
        return consumerRecords;
    }

    private synchronized void pauseTopicPartitions() {
        if (pauseRequests == null || pauseRequests.isEmpty()) {
            return;
        }
        // Only attempt to pause active assignments
        Set<TopicPartition> validPauseRequests = new HashSet<>(pauseRequests);
        validPauseRequests.retainAll(assignments);
        if (validPauseRequests.isEmpty()) {
            return;
        }
        LOG.trace("Pausing Kafka consumption for Consumer [{}] from topic partition: {}", info.clientId, validPauseRequests);
        kafkaConsumer.pause(validPauseRequests);
        LOG.debug("Paused Kafka consumption for Consumer [{}] from topic partition: {}", info.clientId, kafkaConsumer.paused());
        if (pausedTopicPartitions == null) {
            pausedTopicPartitions = new HashSet<>();
        }
        pausedTopicPartitions.addAll(validPauseRequests);
    }

    private synchronized void resumeTopicPartitions() {
        Set<TopicPartition> paused = kafkaConsumer.paused();
        if (paused.isEmpty()) {
            return;
        }
        final List<TopicPartition> toResume = paused.stream()
            .filter(topicPartition -> pauseRequests == null || !pauseRequests.contains(topicPartition))
            .toList();
        if (!toResume.isEmpty()) {
            LOG.debug("Resuming Kafka consumption for Consumer [{}] from topic partition: {}", info.clientId, toResume);
            kafkaConsumer.resume(toResume);
        }
        if (pausedTopicPartitions != null) {
            toResume.forEach(pausedTopicPartitions::remove);
        }
    }

    private ConsumerRecords<?, ?> poll() {
        // Deserialization errors can happen while polling

        // In batch mode, propagate any errors
        if (info.isBatch) {
            return kafkaConsumer.poll(info.pollTimeout);
        }

        // Otherwise, try to honor the configured error strategy
        try {
            return kafkaConsumer.poll(info.pollTimeout);
        } catch (RecordDeserializationException ex) {
            LOG.trace("Kafka consumer [{}] failed to deserialize value while polling", info.logMethod, ex);
            final TopicPartition tp = ex.topicPartition();
            // By default, seek past the record to continue consumption
            kafkaConsumer.seek(tp, ex.offset() + 1);
            // The error strategy and the exception handler can still decide what to do about this record
            resolveWithErrorStrategy(new ConsumerRecord<>(tp.topic(), tp.partition(), ex.offset(), null, null), ex);
            // By now, it's been decided whether this record should be retried and the exception may have been handled
            return null;
        }
    }

    private void processAsBatch(final ConsumerRecords<?, ?> consumerRecords) {
        final Object methodResult = kafkaConsumerProcessor.bindAsBatch(info.method, boundArguments, consumerRecords).invoke(consumerBean);
        normalizeResult(methodResult).ifPresent(result -> {
            final Flux<?> resultFlux = toFlux(result);
            final Iterator<? extends ConsumerRecord<?, ?>> iterator = consumerRecords.iterator();
            final java.util.function.Consumer<Object> consumeNext = o -> {
                if (iterator.hasNext()) {
                    final ConsumerRecord<?, ?> consumerRecord = iterator.next();
                    handleResultFlux(consumerRecord, Flux.just(o), true, consumerRecords);
                }
            };
            if (Publishers.isConvertibleToPublisher(result) && !info.isBlocking) {
                resultFlux.subscribe(consumeNext);
            } else {
                Optional.ofNullable(resultFlux.collectList().block()).stream().flatMap(List::stream).forEach(consumeNext);
            }
        });
        failed = false;
    }

    @Nullable
    private static Optional<Object> normalizeResult(@Nullable Object result) {
        return Optional.ofNullable(result).map(x -> x.getClass().isArray() ? Arrays.asList((Object[]) x) : x);
    }

    private Flux<?> toFlux(Object result) {
        if (result instanceof Iterable<?> iterable) {
            return Flux.fromIterable(iterable);
        }
        if (Publishers.isConvertibleToPublisher(result)) {
            return kafkaConsumerProcessor.convertPublisher(result);
        }
        return Flux.just(result);
    }

    private void processIndividually(final ConsumerRecords<?, ?> consumerRecords) {
        final Map<TopicPartition, OffsetAndMetadata> currentOffsets = info.trackPartitions ? new HashMap<>() : null;
        final Iterator<? extends ConsumerRecord<?, ?>> iterator = consumerRecords.iterator();
        while (iterator.hasNext()) {
            final ConsumerRecord<?, ?> consumerRecord = iterator.next();

            LOG.trace("Kafka consumer [{}] received record: {}", info.logMethod, consumerRecord);

            if (info.trackPartitions) {
                final TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset() + 1, null);
                currentOffsets.put(topicPartition, offsetAndMetadata);
            }

            final KafkaSeekOperations seek = Optional.ofNullable(info.seekArg).map(x -> KafkaSeekOperations.newInstance()).orElse(null);
            Optional.ofNullable(info.seekArg).ifPresent(argument -> boundArguments.put(argument, seek));
            Optional.ofNullable(info.ackArg).ifPresent(argument -> boundArguments.put(argument, (KafkaAcknowledgement) () -> kafkaConsumer.commitSync(currentOffsets)));

            try {
                process(consumerRecord, consumerRecords);
            } catch (Exception e) {
                if (resolveWithErrorStrategy(consumerRecord, e)) {
                    resetTheFollowingPartitions(consumerRecord, iterator);
                    failed = true;
                    return;
                }
            }

            if (info.offsetStrategy == OffsetStrategy.SYNC_PER_RECORD) {
                commitSync(consumerRecord, currentOffsets);
            } else if (info.offsetStrategy == OffsetStrategy.ASYNC_PER_RECORD) {
                kafkaConsumer.commitAsync(currentOffsets, resolveCommitCallback());
            }

            if (seek != null) {
                // Performs seek operations that were deferred by the user
                final KafkaSeeker seeker = KafkaSeeker.newInstance(kafkaConsumer);
                seek.forEach(seeker::perform);
            }
        }
        failed = false;
    }

    private void process(ConsumerRecord<?, ?> consumerRecord, ConsumerRecords<?, ?> consumerRecords) {
        final Object result = kafkaConsumerProcessor.bind(info.method, boundArguments, consumerRecord).invoke(consumerBean);
        if (result != null) {
            final boolean isPublisher = Publishers.isConvertibleToPublisher(result);
            final Flux<?> publisher = isPublisher ? kafkaConsumerProcessor.convertPublisher(result) : Flux.just(result);
            handleResultFlux(consumerRecord, publisher, isPublisher || info.isBlocking, consumerRecords);
        }
    }

    private void commitSync(ConsumerRecord<?, ?> consumerRecord, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            kafkaConsumer.commitSync(currentOffsets);
        } catch (CommitFailedException e) {
            handleException(e, consumerRecord);
        }
    }

    private void handleResultFlux(
        ConsumerRecord<?, ?> consumerRecord,
        Flux<?> publisher,
        boolean isBlocking,
        ConsumerRecords<?, ?> consumerRecords
    ) {
        final Flux<RecordMetadata> recordMetadataProducer = publisher
            .flatMap(value -> sendToDestination(value, consumerRecord, consumerRecords))
            .onErrorResume(error -> handleSendToError(error, consumerRecord));

        if (isBlocking) {
            List<RecordMetadata> listRecords = recordMetadataProducer.collectList().block();
            LOG.trace("Method [{}] produced record metadata: {}", info.method, listRecords);
        } else {
            recordMetadataProducer.subscribe(recordMetadata -> LOG.trace("Method [{}] produced record metadata: {}", info.logMethod, recordMetadata));
        }
    }

    private Publisher<RecordMetadata> sendToDestination(Object value, ConsumerRecord<?, ?> consumerRecord, ConsumerRecords<?, ?> consumerRecords) {
        if (value == null || info.sendToTopics.isEmpty()) {
            return Flux.empty();
        }
        final Object key = consumerRecord.key();
        final Producer<?, ?> kafkaProducer;
        if (info.shouldSendOffsetsToTransaction) {
            kafkaProducer = kafkaConsumerProcessor.getTransactionalProducer(
                info.producerClientId,
                info.producerTransactionalId,
                byte[].class,
                Object.class
            );
        } else {
            kafkaProducer = kafkaConsumerProcessor.getProducer(
                Optional.ofNullable(info.producerClientId).orElse(info.groupId),
                (Class<?>) (key != null ? key.getClass() : byte[].class),
                value.getClass()
            );
        }
        return Flux.create(emitter -> sendToDestination(emitter, kafkaProducer, key, value, consumerRecord, consumerRecords));
    }

    private void sendToDestination(FluxSink<RecordMetadata> emitter, Producer<?, ?> kafkaProducer, Object key, Object value, ConsumerRecord<?, ?> consumerRecord, ConsumerRecords<?, ?> consumerRecords) {
        try {
            if (info.shouldSendOffsetsToTransaction) {
                beginTransaction(kafkaProducer);
            }
            sendToDestination(kafkaProducer, new FluxCallback(emitter), key, value, consumerRecord);
            if (info.shouldSendOffsetsToTransaction) {
                endTransaction(kafkaProducer, consumerRecords);
            }
            emitter.complete();
        } catch (Exception e) {
            if (info.shouldSendOffsetsToTransaction) {
                abortTransaction(kafkaProducer, e);
            }
            emitter.error(e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void sendToDestination(Producer<?, ?> kafkaProducer, Callback callback, Object key, Object value, ConsumerRecord<?, ?> consumerRecord) {
        for (String destinationTopic : info.sendToTopics) {
            if (info.returnsManyKafkaMessages) {
                final Iterable<KafkaMessage> messages = (Iterable<KafkaMessage>) value;
                for (KafkaMessage message : messages) {
                    final ProducerRecord producerRecord = createFromMessage(destinationTopic, message);
                    kafkaProducer.send(producerRecord, callback);
                }
            } else {
                final ProducerRecord producerRecord;
                if (info.returnsOneKafkaMessage) {
                    producerRecord = createFromMessage(destinationTopic, (KafkaMessage) value);
                } else {
                    producerRecord = new ProducerRecord(destinationTopic, null, key, value, consumerRecord.headers());
                }
                LOG.trace("Sending record: {} for producer: {} {}", producerRecord, kafkaProducer, info.producerTransactionalId);
                kafkaProducer.send(producerRecord, callback);
            }
        }
    }

    private void beginTransaction(Producer<?, ?> kafkaProducer) {
        try {
            LOG.trace("Beginning transaction for producer: {}", info.producerTransactionalId);
            kafkaProducer.beginTransaction();
        } catch (ProducerFencedException e) {
            kafkaConsumerProcessor.handleProducerFencedException(kafkaProducer, e);
        }
    }

    private void endTransaction(Producer<?, ?> kafkaProducer, ConsumerRecords<?, ?> consumerRecords) {
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : consumerRecords.partitions()) {
            List<? extends ConsumerRecord<?, ?>> partitionedRecords = consumerRecords.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }
        sendOffsetsToTransaction(kafkaProducer, offsetsToCommit);
    }

    private void abortTransaction(Producer<?, ?> kafkaProducer, Exception e) {
        try {
            LOG.trace("Aborting transaction for producer: {} because of error: {}", info.producerTransactionalId, e.getMessage());
            kafkaProducer.abortTransaction();
        } catch (ProducerFencedException ex) {
            kafkaConsumerProcessor.handleProducerFencedException(kafkaProducer, ex);
        }
    }

    private void sendOffsetsToTransaction(Producer<?, ?> kafkaProducer, Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        try {
            LOG.trace("Sending offsets: {} to transaction for producer: {} and customer group id: {}", offsetsToCommit, info.producerTransactionalId, info.groupId);
            kafkaProducer.sendOffsetsToTransaction(offsetsToCommit, new ConsumerGroupMetadata(info.groupId));
            LOG.trace("Committing transaction for producer: {}", info.producerTransactionalId);
            kafkaProducer.commitTransaction();
            LOG.trace("Committed transaction for producer: {}", info.producerTransactionalId);
        } catch (ProducerFencedException e) {
            kafkaConsumerProcessor.handleProducerFencedException(kafkaProducer, e);
        }
    }

    private Publisher<RecordMetadata> handleSendToError(Throwable error, ConsumerRecord<?, ?> consumerRecord) {
        handleException("Error occurred processing record [" + consumerRecord + "] with Kafka reactive consumer [" + info.method + "]: " + error.getMessage(), error, consumerRecord);

        if (!info.shouldRedeliver) {
            return Flux.empty();
        }

        return redeliver(consumerRecord)
            .doOnError(ex -> handleException("Redelivery failed for record [" + consumerRecord + "] with Kafka reactive consumer [" + info.method + "]: " + error.getMessage(), ex, consumerRecord));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Mono<RecordMetadata> redeliver(ConsumerRecord<?, ?> consumerRecord) {
        final Object key = consumerRecord.key();
        final Object value = consumerRecord.value();

        if (key == null || value == null) {
            return Mono.empty();
        }

        LOG.debug("Attempting redelivery of record [{}] following error", consumerRecord);

        final Producer<?, ?> kafkaProducer = kafkaConsumerProcessor.getProducer(
            Optional.ofNullable(info.producerClientId).orElse(info.groupId),
            key.getClass(),
            value.getClass()
        );

        final ProducerRecord producerRecord = new ProducerRecord(consumerRecord.topic(), consumerRecord.partition(), key, value, consumerRecord.headers());

        LOG.trace("Sending record: {} for producer: {} {}", producerRecord, kafkaProducer, info.producerTransactionalId);
        return Mono.create(emitter -> kafkaProducer.send(producerRecord, new MonoCallback(emitter)));
    }

    private boolean resolveWithErrorStrategy(ConsumerRecord<?, ?> consumerRecord, Throwable e) {

        ErrorStrategyValue currentErrorStrategy = info.errorStrategy;

        if (currentErrorStrategy.isRetry() && !info.exceptionTypes.isEmpty() &&
            info.exceptionTypes.stream().noneMatch(error -> error.equals(e.getClass()))) {
            if (partitionRetries != null) {
                partitionRetries.remove(consumerRecord.partition());
            }
            // Skip the failing record
            currentErrorStrategy = ErrorStrategyValue.RESUME_AT_NEXT_RECORD;
        }

        if (currentErrorStrategy.isRetry() && info.retryCount != 0) {

            final PartitionRetryState retryState = getPartitionRetryState(consumerRecord);

            if (info.retryCount >= retryState.currentRetryCount) {
                if (info.shouldHandleAllExceptions) {
                    handleException(e, consumerRecord);
                }

                final TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                kafkaConsumer.seek(topicPartition, consumerRecord.offset());

                final Duration retryDelay = currentErrorStrategy.computeRetryDelay(info.retryDelay, retryState.currentRetryCount);
                if (retryDelay != null) {
                    // in the stop on error strategy, pause the consumer and resume after the retryDelay duration
                    final Set<TopicPartition> paused = Collections.singleton(topicPartition);
                    pause(paused);
                    kafkaConsumerProcessor.scheduleTask(retryDelay, () -> resume(paused));
                }
                return true;
            } else {
                partitionRetries.remove(consumerRecord.partition());
                // Skip the failing record
                currentErrorStrategy = ErrorStrategyValue.RESUME_AT_NEXT_RECORD;
            }
        }

        handleException(e, consumerRecord);

        return currentErrorStrategy != ErrorStrategyValue.RESUME_AT_NEXT_RECORD;
    }

    private PartitionRetryState getPartitionRetryState(ConsumerRecord<?, ?> consumerRecord) {
        final PartitionRetryState retryState;
        if (partitionRetries == null) {
            partitionRetries = new HashMap<>();
        }
        retryState = partitionRetries.computeIfAbsent(consumerRecord.partition(), x -> new PartitionRetryState());
        if (retryState.currentRetryOffset != consumerRecord.offset()) {
            retryState.currentRetryOffset = consumerRecord.offset();
            retryState.currentRetryCount = 1;
        } else {
            retryState.currentRetryCount++;
        }
        return retryState;
    }

    private void resetTheFollowingPartitions(
        ConsumerRecord<?, ?> errorConsumerRecord,
        Iterator<? extends ConsumerRecord<?, ?>> iterator
    ) {
        Set<Integer> processedPartition = new HashSet<>();
        processedPartition.add(errorConsumerRecord.partition());
        while (iterator.hasNext()) {
            ConsumerRecord<?, ?> consumerRecord = iterator.next();
            if (!processedPartition.add(consumerRecord.partition())) {
                continue;
            }
            TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
            kafkaConsumer.seek(topicPartition, consumerRecord.offset());
        }
    }

    private void handleException(Throwable e, ConsumerRecord<?, ?> consumerRecord) {
        kafkaConsumerProcessor.handleException(consumerBean,
            new KafkaListenerException(e, consumerBean, kafkaConsumer, consumerRecord));
    }

    private void handleException(String message, Throwable e, ConsumerRecord<?, ?> consumerRecord) {
        kafkaConsumerProcessor.handleException(consumerBean,
            new KafkaListenerException(message, e, consumerBean, kafkaConsumer, consumerRecord));
    }

    private OffsetCommitCallback resolveCommitCallback() {
        return (offsets, exception) -> {
            if (consumerBean instanceof OffsetCommitCallback occ) {
                occ.onComplete(offsets, exception);
            } else if (exception != null) {
                LOG.error("Error asynchronously committing Kafka offsets [{}]: {}", offsets, exception.getMessage(), exception);
            }
        };
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static ProducerRecord createFromMessage(String topic, KafkaMessage<?, ?> message) {
        return new ProducerRecord(
            Optional.ofNullable(message.getTopic()).orElse(topic),
            message.getPartition(),
            message.getTimestamp(),
            message.getKey(),
            message.getBody(),
            Optional.ofNullable(message.getHeaders()).map(ConsumerState::convertHeaders).orElse(null)
        );
    }

    private static List<RecordHeader> convertHeaders(Map<String, Object> headers) {
        return headers.entrySet().stream()
            .map(e -> new RecordHeader(e.getKey(), e.getValue().toString().getBytes(StandardCharsets.UTF_8)))
            .toList();
    }
}
