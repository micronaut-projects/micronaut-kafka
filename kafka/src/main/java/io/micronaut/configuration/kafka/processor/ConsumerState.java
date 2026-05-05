/*
 * Copyright 2017-2026 original authors
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
import io.micronaut.configuration.kafka.exceptions.OffsetCommitExceptionLogger;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException;
import io.micronaut.configuration.kafka.scope.KafkaCustomScope;
import io.micronaut.core.annotation.Internal;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import io.micronaut.core.type.Argument;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * The internal state of the consumer.
 *
 * @author Denis Stepanov
 * @since 5.2
 */
@Internal
abstract class ConsumerState {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerProcessor.class); // NOSONAR
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DLQ_PUBLISH_TIMEOUT = Duration.ofSeconds(5);
    private static final String DLQ_EXCEPTION_CLASS_HEADER = "micronaut-kafka-exception-class";
    private static final String DLQ_EXCEPTION_MESSAGE_HEADER = "micronaut-kafka-exception-message";
    private static final String DLQ_ORIGINAL_TOPIC_HEADER = "micronaut-kafka-original-topic";
    private static final String DLQ_ORIGINAL_PARTITION_HEADER = "micronaut-kafka-original-partition";
    private static final String DLQ_ORIGINAL_OFFSET_HEADER = "micronaut-kafka-original-offset";

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
    private CountDownLatch startupLatch;
    private final CountDownLatch closedLatch;
    private boolean pollingStarted;
    private volatile ConsumerCloseState closedState;
    private volatile boolean shutdownRequested;
    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

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
        this.startupLatch = info.autoStartup ? null : new CountDownLatch(1);
        this.boundArguments = new HashMap<>(2);
        this.closedState = ConsumerCloseState.NOT_STARTED;
        this.closedLatch = new CountDownLatch(1);
        this.topicPartitionRetries = this.info.errorStrategy.isRetry() ? new HashMap<>() : null;
    }

    protected abstract ConsumerRecords<?, ?> pollRecords(@Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets); // NOSONAR

    protected abstract void processRecords(ConsumerRecords<?, ?> consumerRecords, Map<TopicPartition, OffsetAndMetadata> currentOffsets); // NOSONAR

    @Nullable
    protected abstract Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets();

    protected final <T> T withKafkaScope(Supplier<T> action) {
        KafkaCustomScope kafkaScope = kafkaConsumerProcessor == null ? null : kafkaConsumerProcessor.getKafkaScope();
        if (kafkaScope == null) {
            return action.get();
        }
        return kafkaScope.execute(action);
    }

    protected final void withKafkaScope(Runnable action) {
        withKafkaScope(() -> {
            action.run();
            return null;
        });
    }

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
        pauseRequests = null;
        if (startupLatch != null) {
            startupLatch.countDown();
        }
    }

    synchronized void resume(@NonNull Collection<TopicPartition> topicPartitions) {
        if (pauseRequests != null) {
            pauseRequests.removeAll(topicPartitions);
        }
        if (startupLatch != null) {
            startupLatch.countDown();
        }
    }

    synchronized boolean isPaused(@NonNull Collection<TopicPartition> topicPartitions) {
        if (startupLatch != null && startupLatch.getCount() > 0) {
            return true;
        }
        if (pauseRequests == null || pausedTopicPartitions == null) {
            return false;
        }
        return pauseRequests.containsAll(topicPartitions) && pausedTopicPartitions.containsAll(topicPartitions);
    }

    void wakeUp() {
        kafkaConsumer.wakeup();
        synchronized (this) {
            if (startupLatch != null) {
                startupLatch.countDown();
            }
        }
    }

    void requestShutdown() {
        shutdownRequested = true;
    }

    CompletableFuture<Void> getShutdownFuture() {
        return shutdownFuture;
    }

    boolean isActive() {
        return !shutdownFuture.isDone();
    }

    void close() {
        boolean closed = closedState == ConsumerCloseState.CLOSED;
        if (!closed && (pollingStarted || closedState == ConsumerCloseState.POLLING)) {
            final Instant start = Instant.now();
            final Duration timeout = getCloseTimeout();
            try {
                closed = closedLatch.await(Math.max(0, timeout.toMillis()), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!closed) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Consumer {} is not closed yet (waiting {})", info.clientId, Duration.between(start, Instant.now()));
                }
                LOG.warn("Consumer {} was not closed after waiting {}", info.clientId, timeout);
            }
        }
        LOG.debug("Consumer {} is {}", info.clientId, closedState == ConsumerCloseState.CLOSED ? "closed" : "not closed");
    }

    void threadPollLoop() {
        try (kafkaConsumer) {
            holdStartup();
            while (!shutdownRequested) {
                refreshAssignmentsPollAndProcessRecords();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (WakeupException e) {
            // Closing a Kafka consumer relies on wakeup to break a blocked poll.
            LOG.debug("Consumer {} woken up during shutdown", info.clientId);
        } finally {
            closeComplete();
        }
    }

    private void holdStartup() throws InterruptedException {
        if (startupLatch != null) {
            startupLatch.await();
            synchronized (this) {
                startupLatch = null;
            }
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

    private void closeComplete() {
        closedState = ConsumerCloseState.CLOSED;
        closedLatch.countDown();
        shutdownFuture.complete(null);
    }

    @NonNull
    protected Duration getCloseTimeout() {
        return CLOSE_TIMEOUT;
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
        String topic,
        Flux<?> publisher,
        boolean isBlocking
    ) {
        final Flux<RecordMetadata> recordMetadataProducer = publisher
            .flatMap(value -> sendToDestination(topic, value, consumerRecord, consumerRecords));

        if (isBlocking) {
            List<RecordMetadata> listRecords = recordMetadataProducer.collectList().block();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Method [{}] produced record metadata: {}", info.logMethod(topic), listRecords);
            }
        } else if (LOG.isTraceEnabled()) {
            recordMetadataProducer.subscribe(recordMetadata -> LOG.trace("Method [{}] produced record metadata: {}", info.logMethod(topic), recordMetadata));
        } else {
            recordMetadataProducer.subscribe();
        }
    }

    private Publisher<RecordMetadata> sendToDestination(String topic, Object value, ConsumerRecord<?, ?> consumerRecord, ConsumerRecords<?, ?> consumerRecords) {
        if (value == null || info.sendToTopics(topic).isEmpty()) {
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
        Flux<RecordMetadata> result = Flux.create(emitter -> sendToDestination(emitter, kafkaProducer, topic, key, value, consumerRecord, consumerRecords));
        return result.onErrorResume(error -> handleSendToError(topic, error, consumerRecords, consumerRecord));
    }

    private void sendToDestination(
        FluxSink<RecordMetadata> emitter,
        Producer<?, ?> kafkaProducer,
        String topic,
        Object key,
        Object value,
        ConsumerRecord<?, ?> consumerRecord,
        ConsumerRecords<?, ?> consumerRecords
    ) {
        try {
            if (info.shouldSendOffsetsToTransaction) {
                beginTransaction(kafkaProducer);
            }
            sendToDestination(kafkaProducer, new FluxCallback(emitter), topic, key, value, consumerRecord);
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
    private void sendToDestination(Producer<?, ?> kafkaProducer, Callback callback, String topic, Object key, Object value, ConsumerRecord<?, ?> consumerRecord) {
        for (String destinationTopic : info.sendToTopics(topic)) {
            if (info.returnsManyKafkaMessages(topic)) {
                final Iterable<KafkaMessage> messages = (Iterable<KafkaMessage>) value;
                for (KafkaMessage message : messages) {
                    final ProducerRecord producerRecord = createFromMessage(destinationTopic, message);
                    kafkaProducer.send(producerRecord, callback);
                }
            } else {
                final ProducerRecord producerRecord;
                if (info.returnsOneKafkaMessage(topic)) {
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

    private Publisher<RecordMetadata> handleSendToError(String topic, Throwable error, ConsumerRecords<?, ?> consumerRecords, ConsumerRecord<?, ?> consumerRecord) {
        handleException("Error occurred processing record [" + consumerRecord + "] with Kafka reactive consumer [" + info.methodForTopic(topic) + "]: " + error.getMessage(), error, consumerRecords, consumerRecord);

        if (!info.shouldRedeliver) {
            return Flux.empty();
        }

        return redeliver(consumerRecord)
            .doOnError(ex -> handleException("Redelivery failed for record [" + consumerRecord + "] with Kafka reactive consumer [" + info.methodForTopic(topic) + "]: " + error.getMessage(), ex, consumerRecords, consumerRecord));
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
            return kafkaConsumerProcessor.shouldRetryMessage(consumerBean, wrapExceptionInKafkaListenerException(e.getMessage(), e, consumerRecords, consumerRecord)) ||
                info.exceptionTypes.stream().anyMatch(e.getClass()::equals);
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

    protected void publishToDlq(Throwable e, @Nullable ConsumerRecords<?, ?> consumerRecords,
        @Nullable ConsumerRecord<?, ?> consumerRecord) {
        if (info.errorStrategy != io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.LOG_AND_RESUME_AT_NEXT_RECORD || info.dlq == null) {
            return;
        }
        if (consumerRecord != null) {
            publishConsumerRecordToDlq(consumerRecord, e);
        } else if (consumerRecords != null) {
            for (ConsumerRecord<?, ?> failedRecord : consumerRecords) {
                publishConsumerRecordToDlq(failedRecord, e);
            }
        }
    }

    private void handleException(String message, Throwable e, @Nullable ConsumerRecords<?, ?> consumerRecords, @Nullable ConsumerRecord<?, ?> consumerRecord) {
        kafkaConsumerProcessor.handleException(consumerBean,
            wrapExceptionInKafkaListenerException(message, e, consumerRecords, consumerRecord));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void publishConsumerRecordToDlq(ConsumerRecord<?, ?> consumerRecord, Throwable error) {
        final Object key = consumerRecord.key();
        final Object value = consumerRecord.value();
        final Producer<?, ?> kafkaProducer = kafkaConsumerProcessor.getProducer(
            Optional.ofNullable(info.producerClientId).orElse(info.groupId),
            (Class<?>) (key != null ? key.getClass() : byte[].class),
            (Class<?>) (value != null ? value.getClass() : byte[].class)
        );
        final Headers headers = new RecordHeaders(consumerRecord.headers());
        addDlqHeader(headers, DLQ_EXCEPTION_CLASS_HEADER, error.getClass().getName());
        addDlqHeader(headers, DLQ_EXCEPTION_MESSAGE_HEADER, error.getMessage());
        addDlqHeader(headers, DLQ_ORIGINAL_TOPIC_HEADER, consumerRecord.topic());
        addDlqHeader(headers, DLQ_ORIGINAL_PARTITION_HEADER, Integer.toString(consumerRecord.partition()));
        addDlqHeader(headers, DLQ_ORIGINAL_OFFSET_HEADER, Long.toString(consumerRecord.offset()));
        final Long timestamp = consumerRecord.timestamp() >= 0 ? consumerRecord.timestamp() : null;
        final ProducerRecord producerRecord = new ProducerRecord(info.dlq, null, timestamp, key, value, headers);
        final long dlqPublishTimeoutSeconds = DLQ_PUBLISH_TIMEOUT.toSeconds();
        try {
            kafkaProducer.send(producerRecord).get(dlqPublishTimeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException dlqError) {
            LOG.error(
                "Timed out publishing record [topic={}, partition={}, offset={}, headers={}] to DLQ [{}] after {} s",
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.headers().toArray().length,
                info.dlq,
                dlqPublishTimeoutSeconds,
                dlqError
            );
        } catch (InterruptedException dlqError) {
            Thread.currentThread().interrupt();
            LOG.error(
                "Error publishing record [topic={}, partition={}, offset={}, headers={}] to DLQ [{}]: {}",
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.headers().toArray().length,
                info.dlq,
                dlqError.getMessage(),
                dlqError
            );
        } catch (Exception dlqError) {
            LOG.error(
                "Error publishing record [topic={}, partition={}, offset={}, headers={}] to DLQ [{}]: {}",
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.headers().toArray().length,
                info.dlq,
                dlqError.getMessage(),
                dlqError
            );
        }
    }

    private static void addDlqHeader(Headers headers, String name, @Nullable String value) {
        headers.remove(name);
        if (value != null) {
            headers.add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    private KafkaListenerException wrapExceptionInKafkaListenerException(String message, Throwable e, @Nullable ConsumerRecords<?, ?> consumerRecords, @Nullable ConsumerRecord<?, ?> consumerRecord) {
        return new KafkaListenerException(message, e, consumerBean, kafkaConsumer, consumerRecords, consumerRecord, info.cooperativeStickyAssignmentStrategy);
    }

    private OffsetCommitCallback resolveCommitCallback() {
        return (offsets, exception) -> {
            if (consumerBean instanceof OffsetCommitCallback occ) {
                occ.onComplete(offsets, exception);
            } else if (exception != null) {
                OffsetCommitExceptionLogger.log(LOG, info.cooperativeStickyAssignmentStrategy,
                    "Error asynchronously committing Kafka offsets [{}]: {}", exception, offsets, exception.getMessage());
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
