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
package io.micronaut.configuration.kafka;

import org.awaitility.Awaitility;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecoveringTransactionalProducerTest {
    private static final ProducerRecord<String, String> RECORD = new ProducerRecord<>("topic", "value");

    @Test
    void delegatesNonTransactionalOperationsToCurrentProducer() throws Exception {
        StubProducer producer = new StubProducer().enqueueSend(SendBehavior.success());
        RecoveringTransactionalProducer<String, String> recoveringProducer =
            new RecoveringTransactionalProducer<>(() -> producer, "tx");

        recoveringProducer.initTransactions();
        Future<RecordMetadata> future = recoveringProducer.send(RECORD);

        assertNotNull(assertDoesNotThrow(() -> future.get()));
        recoveringProducer.flush();
        assertSame(producer.partitions, recoveringProducer.partitionsFor("topic"));
        assertSame(producer.metrics, recoveringProducer.metrics());
        assertSame(producer.clientInstanceId, recoveringProducer.clientInstanceId(Duration.ofSeconds(1)));
        recoveringProducer.registerMetricForSubscription(null);
        recoveringProducer.unregisterMetricFromSubscription(null);
        recoveringProducer.close(Duration.ofSeconds(5));
        recoveringProducer.close(Duration.ofSeconds(1));

        assertEquals(1, producer.initTransactionsCount);
        assertEquals(1, producer.sendRecords.size());
        assertEquals(1, producer.flushCount);
        assertEquals(1, producer.registerMetricCount);
        assertEquals(1, producer.unregisterMetricCount);
        assertEquals(List.of(Duration.ofSeconds(5)), producer.closeTimeouts);
    }

    @Test
    void recreatesProducerWhenBeginTransactionFailsWithExpiredPid() {
        StubProducer first = new StubProducer().enqueueBeginException(new InvalidPidMappingException("expired"));
        StubProducer second = new StubProducer();
        RecoveringTransactionalProducer<String, String> recoveringProducer =
            new RecoveringTransactionalProducer<>(supplier(first, second), "tx");

        recoveringProducer.initTransactions();
        recoveringProducer.beginTransaction();

        assertEquals(1, first.beginTransactionCount);
        assertEquals(1, first.initTransactionsCount);
        assertEquals(List.of(Duration.ZERO), first.closeTimeouts);
        assertEquals(1, second.initTransactionsCount);
        assertEquals(1, second.beginTransactionCount);
    }

    @Test
    void replaysPendingTransactionalSendsWhenCallbackSignalsExpiredProducer() throws Exception {
        StubProducer first = new StubProducer().enqueueSend(SendBehavior.callbackFailure(new InvalidPidMappingException("expired")));
        StubProducer second = new StubProducer().enqueueSend(SendBehavior.success());
        RecoveringTransactionalProducer<String, String> recoveringProducer =
            new RecoveringTransactionalProducer<>(supplier(first, second), "tx");
        AtomicReference<RecordMetadata> callbackMetadata = new AtomicReference<>();
        AtomicReference<Exception> callbackException = new AtomicReference<>();

        recoveringProducer.initTransactions();
        recoveringProducer.beginTransaction();
        Future<RecordMetadata> future = recoveringProducer.send(RECORD, (metadata, exception) -> {
            callbackMetadata.set(metadata);
            callbackException.set(exception);
        });
        recoveringProducer.commitTransaction();

        RecordMetadata metadata = future.get();
        assertNotNull(metadata);
        assertSame(metadata, callbackMetadata.get());
        assertEquals(null, callbackException.get());
        assertEquals(1, first.sendRecords.size());
        assertEquals(1, second.sendRecords.size());
        assertEquals(1, second.beginTransactionCount);
        assertEquals(1, second.commitTransactionCount);
        assertEquals(List.of(Duration.ZERO), first.closeTimeouts);
    }

    @Test
    void retriesOffsetsAndCommitAfterRecoverableProducerExpiration() throws Exception {
        StubProducer first = new StubProducer()
            .enqueueSend(SendBehavior.success())
            .enqueueSendOffsetsException(new InvalidPidMappingException("expired"));
        StubProducer second = new StubProducer().enqueueSend(SendBehavior.success());
        RecoveringTransactionalProducer<String, String> recoveringProducer =
            new RecoveringTransactionalProducer<>(supplier(first, second), "tx");
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(1L));

        recoveringProducer.initTransactions();
        recoveringProducer.beginTransaction();
        Future<RecordMetadata> future = recoveringProducer.send(RECORD);
        recoveringProducer.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata("group"));
        recoveringProducer.commitTransaction();

        assertNotNull(assertDoesNotThrow(() -> future.get()));
        assertEquals(1, first.sendOffsetsToTransactionCount);
        assertEquals(1, second.sendOffsetsToTransactionCount);
        assertEquals(1, second.commitTransactionCount);
        assertEquals(1, second.sendRecords.size());
        assertEquals(List.of(Duration.ZERO), first.closeTimeouts);
    }

    @Test
    void abortAndCloseClearStateAndRejectFurtherUse() {
        StubProducer producer = new StubProducer().enqueueSend(SendBehavior.success());
        RecoveringTransactionalProducer<String, String> recoveringProducer =
            new RecoveringTransactionalProducer<>(() -> producer, "tx");

        recoveringProducer.initTransactions();
        recoveringProducer.beginTransaction();
        assertDoesNotThrow(() -> recoveringProducer.send(RECORD).get());
        recoveringProducer.abortTransaction();
        recoveringProducer.close();

        IllegalStateException exception = assertThrows(IllegalStateException.class, recoveringProducer::initTransactions);
        assertTrue(exception.getMessage().contains("already closed"));
        assertEquals(1, producer.abortTransactionCount);
        assertEquals(List.of(Duration.ofSeconds(30)), producer.closeTimeouts);
    }

    @Test
    void propagatesSynchronousSendFailuresAndWrappedCheckedFailures() {
        StubProducer producer = new StubProducer().enqueueSend(SendBehavior.throwing(new KafkaException("boom")));
        RecoveringTransactionalProducer<String, String> recoveringProducer =
            new RecoveringTransactionalProducer<>(() -> producer, "tx");

        recoveringProducer.initTransactions();
        recoveringProducer.beginTransaction();

        ExecutionException sendException = assertThrows(ExecutionException.class, () -> recoveringProducer.send(RECORD).get());
        assertInstanceOf(KafkaException.class, sendException.getCause());
        assertTrue(sendException.getCause().getMessage().contains("boom"));
        recoveringProducer.abortTransaction();

        RecoveringTransactionalProducer<String, String> checkedFailureProducer =
            new RecoveringTransactionalProducer<>(() -> new StubProducer().enqueueSend(SendBehavior.checkedFailure()), "tx");
        checkedFailureProducer.initTransactions();
        checkedFailureProducer.beginTransaction();
        checkedFailureProducer.send(RECORD);

        KafkaException kafkaException = assertThrows(KafkaException.class, checkedFailureProducer::commitTransaction);
        assertTrue(kafkaException.getMessage().contains("Transactional send failed"));
        assertInstanceOf(Exception.class, kafkaException.getCause());
    }

    @Test
    void notifiesCallbacksWhenReplayRecoveryFails() {
        StubProducer first = new StubProducer().enqueueSend(SendBehavior.callbackFailure(new InvalidPidMappingException("expired")));
        StubProducer second = new StubProducer().enqueueBeginException(new KafkaException("recovery failed"));
        RecoveringTransactionalProducer<String, String> recoveringProducer =
            new RecoveringTransactionalProducer<>(supplier(first, second), "tx");
        AtomicReference<Exception> callbackException = new AtomicReference<>();

        recoveringProducer.initTransactions();
        recoveringProducer.beginTransaction();
        Future<RecordMetadata> future = recoveringProducer.send(RECORD, (metadata, exception) -> callbackException.set(exception));

        ExecutionException exception = assertThrows(ExecutionException.class, future::get);

        assertInstanceOf(KafkaException.class, exception.getCause());
        assertTrue(exception.getCause().getMessage().contains("recovery failed"));
        Awaitility.await().untilAsserted(() -> assertInstanceOf(KafkaException.class, callbackException.get()));
        assertTrue(callbackException.get().getMessage().contains("recovery failed"));
    }

    private static java.util.function.Supplier<Producer<String, String>> supplier(StubProducer... producers) {
        Deque<StubProducer> deque = new ArrayDeque<>(List.of(producers));
        return deque::removeFirst;
    }

    private static final class StubProducer implements Producer<String, String> {
        private final Deque<RuntimeException> beginExceptions = new ArrayDeque<>();
        private final Deque<RuntimeException> sendOffsetsExceptions = new ArrayDeque<>();
        private final Deque<RuntimeException> commitExceptions = new ArrayDeque<>();
        private final Deque<SendBehavior> sendBehaviors = new ArrayDeque<>();
        private final List<ProducerRecord<String, String>> sendRecords = new ArrayList<>();
        private final List<Duration> closeTimeouts = new ArrayList<>();
        private final List<PartitionInfo> partitions = List.of(new PartitionInfo("topic", 0, null, null, null));
        private final Map<MetricName, Metric> metrics = Map.of();
        private final Uuid clientInstanceId = Uuid.randomUuid();

        private int initTransactionsCount;
        private int beginTransactionCount;
        private int sendOffsetsToTransactionCount;
        private int commitTransactionCount;
        private int abortTransactionCount;
        private int flushCount;
        private int registerMetricCount;
        private int unregisterMetricCount;

        private StubProducer enqueueBeginException(RuntimeException exception) {
            beginExceptions.add(exception);
            return this;
        }

        private StubProducer enqueueSendOffsetsException(RuntimeException exception) {
            sendOffsetsExceptions.add(exception);
            return this;
        }

        private StubProducer enqueueCommitException(RuntimeException exception) {
            commitExceptions.add(exception);
            return this;
        }

        private StubProducer enqueueSend(SendBehavior behavior) {
            sendBehaviors.add(behavior);
            return this;
        }

        @Override
        public void initTransactions() {
            initTransactionsCount++;
        }

        @Override
        public void beginTransaction() {
            beginTransactionCount++;
            if (!beginExceptions.isEmpty()) {
                throw beginExceptions.removeFirst();
            }
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
            sendOffsetsToTransactionCount++;
            if (!sendOffsetsExceptions.isEmpty()) {
                throw sendOffsetsExceptions.removeFirst();
            }
        }

        @Override
        public void commitTransaction() {
            commitTransactionCount++;
            if (!commitExceptions.isEmpty()) {
                throw commitExceptions.removeFirst();
            }
        }

        @Override
        public void abortTransaction() {
            abortTransactionCount++;
        }

        @Override
        public void registerMetricForSubscription(KafkaMetric metric) {
            registerMetricCount++;
        }

        @Override
        public void unregisterMetricFromSubscription(KafkaMetric metric) {
            unregisterMetricCount++;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
            return send(record, null);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
            sendRecords.add(record);
            SendBehavior behavior = sendBehaviors.isEmpty() ? SendBehavior.success() : sendBehaviors.removeFirst();
            return switch (behavior.kind) {
                case SUCCESS -> complete(record, callback);
                case CALLBACK_FAILURE -> failWithCallback(callback, behavior.callbackException);
                case THROW -> throw behavior.runtimeException;
                case CHECKED_FAILURE -> completeExceptionally(callback, new Exception("checked failure"));
            };
        }

        @Override
        public void flush() {
            flushCount++;
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return partitions;
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return metrics;
        }

        @Override
        public Uuid clientInstanceId(Duration timeout) {
            return clientInstanceId;
        }

        @Override
        public void close() {
            close(Duration.ofSeconds(30));
        }

        @Override
        public void close(Duration timeout) {
            closeTimeouts.add(timeout);
        }

        private Future<RecordMetadata> complete(ProducerRecord<String, String> record, Callback callback) {
            RecordMetadata metadata = new RecordMetadata(new TopicPartition(record.topic(), 0), 0, 0, 0L, 0, 0);
            if (callback != null) {
                callback.onCompletion(metadata, null);
            }
            return CompletableFuture.completedFuture(metadata);
        }

        private Future<RecordMetadata> failWithCallback(Callback callback, Exception exception) {
            if (callback != null) {
                callback.onCompletion(null, exception);
            }
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            return future;
        }

        private Future<RecordMetadata> completeExceptionally(Callback callback, Exception exception) {
            if (callback != null) {
                callback.onCompletion(null, exception);
            }
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            return future;
        }
    }

    private static final class SendBehavior {
        private final Kind kind;
        private final RuntimeException runtimeException;
        private final Exception callbackException;

        private SendBehavior(Kind kind, RuntimeException runtimeException, Exception callbackException) {
            this.kind = kind;
            this.runtimeException = runtimeException;
            this.callbackException = callbackException;
        }

        private static SendBehavior success() {
            return new SendBehavior(Kind.SUCCESS, null, null);
        }

        private static SendBehavior callbackFailure(Exception exception) {
            return new SendBehavior(Kind.CALLBACK_FAILURE, null, exception);
        }

        private static SendBehavior throwing(RuntimeException exception) {
            return new SendBehavior(Kind.THROW, exception, null);
        }

        private static SendBehavior checkedFailure() {
            return new SendBehavior(Kind.CHECKED_FAILURE, null, null);
        }

        private enum Kind {
            SUCCESS,
            CALLBACK_FAILURE,
            THROW,
            CHECKED_FAILURE
        }
    }
}
