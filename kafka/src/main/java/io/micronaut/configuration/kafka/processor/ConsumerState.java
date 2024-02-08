/*
 * Copyright 2017-2024 original authors
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

import io.micronaut.configuration.kafka.KafkaMessage;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.type.Argument;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
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
 * @since 5.2
 */
@Internal
abstract class ConsumerState {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerProcessor.class); // NOSONAR

    protected final KafkaConsumerProcessor kafkaConsumerProcessor;
    protected final Object consumerBean;
    @Nullable
    protected final Map<TopicPartition, PartitionRetryState> topicPartitionRetries;
    protected final Map<Argument<?>, Object> boundArguments;
    protected boolean failed;
    final ConsumerInfo info;
    final Consumer<?, ?> kafkaConsumer;
    final Set<String> subscriptions;
    Set<TopicPartition> assignments;
    private Set<TopicPartition> pausedTopicPartitions;
    private Set<TopicPartition> pauseRequests;
    private boolean autoPaused;
    private boolean pollingStarted;
    private volatile ConsumerCloseState closedState;

    protected ConsumerState(
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
        this.topicPartitionRetries = this.info.errorStrategy.isRetry() ? new HashMap<>() : null;
    }

    protected abstract ConsumerRecords<?, ?> pollRecords(@Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets); // NOSONAR

    protected abstract void processRecords(ConsumerRecords<?, ?> consumerRecords, Map<TopicPartition, OffsetAndMetadata> currentOffsets); // NOSONAR

    @Nullable
    protected abstract Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets();

    void pause() {
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

    void wakeUp() {
        kafkaConsumer.wakeup();
    }

    void close() {
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

    void threadPollLoop() {
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
            handleException(e, null, null);
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
        // We need to retrieve current offsets in case we need to retry the current record or batch
        final Map<TopicPartition, OffsetAndMetadata> currentOffsets = getCurrentOffsets();
        // Poll records
        pauseTopicPartitions();
        final ConsumerRecords<?, ?> consumerRecords = pollRecords(currentOffsets);
        closedState = ConsumerCloseState.POLLING;
        if (!pollingStarted) {
            pollingStarted = true;
            kafkaConsumerProcessor.publishStartedPollingEvent(kafkaConsumer);
        }
        resumeTopicPartitions();
        if (consumerRecords == null || consumerRecords.isEmpty()) {
            return; // No consumer records to process
        }
        // Support Kotlin coroutines
        if (info.method.isSuspend()) {
            Argument<?> lastArgument = info.method.getArguments()[info.method.getArguments().length - 1];
            boundArguments.put(lastArgument, null);
        }
        processRecords(consumerRecords, currentOffsets);
        if (failed) {
            return;
        }
        if (info.offsetStrategy == OffsetStrategy.SYNC) {
            try {
                kafkaConsumer.commitSync();
            } catch (CommitFailedException e) {
                handleException(e, consumerRecords, null);
            }
        } else if (info.offsetStrategy == OffsetStrategy.ASYNC) {
            kafkaConsumer.commitAsync(resolveCommitCallback());
        }
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

    protected void handleResultFlux(
        ConsumerRecords<?, ?> consumerRecords,
        ConsumerRecord<?, ?> consumerRecord,
        Flux<?> publisher,
        boolean isBlocking
    ) {
        final Flux<RecordMetadata> recordMetadataProducer = publisher
            .flatMap(value -> sendToDestination(value, consumerRecord, consumerRecords))
            .onErrorResume(error -> handleSendToError(error, consumerRecords, consumerRecord));

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

    private Publisher<RecordMetadata> handleSendToError(Throwable error, ConsumerRecords<?, ?> consumerRecords, ConsumerRecord<?, ?> consumerRecord) {
        handleException("Error occurred processing record [" + consumerRecord + "] with Kafka reactive consumer [" + info.method + "]: " + error.getMessage(), error, consumerRecords, consumerRecord);

        if (!info.shouldRedeliver) {
            return Flux.empty();
        }

        return redeliver(consumerRecord)
            .doOnError(ex -> handleException("Redelivery failed for record [" + consumerRecord + "] with Kafka reactive consumer [" + info.method + "]: " + error.getMessage(), ex, consumerRecords, consumerRecord));
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

    protected void delayRetry(int currentRetryCount, Set<TopicPartition> partitions) {
        // Decide how long should we wait to retry this batch again
        final Duration retryDelay = info.errorStrategy.computeRetryDelay(info.retryDelay,
            currentRetryCount);
        if (retryDelay != null) {
            pause(partitions);
            kafkaConsumerProcessor.scheduleTask(retryDelay, () -> resume(partitions));
        }
    }

    protected boolean shouldRetryException(Throwable e, ConsumerRecords<?, ?> consumerRecords, ConsumerRecord<?, ?> consumerRecord) {
        if (info.errorStrategy.isConditionalRetry()) {
            return kafkaConsumerProcessor.shouldRetryMessage(consumerBean, wrapExceptionInKafkaListenerException(e.getMessage(), e, consumerRecords, consumerRecord));
        }

        return info.exceptionTypes.isEmpty() ||
            info.exceptionTypes.stream().anyMatch(e.getClass()::equals);
    }

    protected PartitionRetryState getPartitionRetryState(TopicPartition tp, long currentOffset) {
        final PartitionRetryState retryState = topicPartitionRetries
            .computeIfAbsent(tp, x -> new PartitionRetryState());
        if (retryState.currentRetryOffset != currentOffset) {
            retryState.currentRetryOffset = currentOffset;
            retryState.currentRetryCount = 1;
        } else {
            retryState.currentRetryCount++;
        }
        return retryState;
    }

    protected void handleException(Throwable e, @Nullable ConsumerRecords<?, ?> consumerRecords,
        @Nullable ConsumerRecord<?, ?> consumerRecord) {
        handleException(e.getMessage(), e, consumerRecords, consumerRecord);
    }

    private void handleException(String message, Throwable e, @Nullable ConsumerRecords<?, ?> consumerRecords, @Nullable ConsumerRecord<?, ?> consumerRecord) {
        kafkaConsumerProcessor.handleException(consumerBean,
            wrapExceptionInKafkaListenerException(message, e, consumerRecords, consumerRecord));
    }

    private KafkaListenerException wrapExceptionInKafkaListenerException(String message, Throwable e, @Nullable ConsumerRecords<?, ?> consumerRecords, @Nullable ConsumerRecord<?, ?> consumerRecord) {
        return new KafkaListenerException(message, e, consumerBean, kafkaConsumer, consumerRecords, consumerRecord);
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
