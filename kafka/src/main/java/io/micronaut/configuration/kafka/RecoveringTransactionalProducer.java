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

import io.micronaut.core.annotation.Internal;
import org.jspecify.annotations.Nullable;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * Wraps a transactional Kafka producer and recreates it when Kafka expires the transactional id.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
@Internal
public final class RecoveringTransactionalProducer<K, V> implements Producer<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(RecoveringTransactionalProducer.class);

    private final Supplier<Producer<K, V>> producerSupplier;
    private final @Nullable String transactionalId;
    private final List<PendingSend<K, V>> pendingSends = new ArrayList<>();
    private @Nullable ExecutorService recoveryExecutor;
    private @Nullable Producer<K, V> producer;
    private boolean closed;
    private boolean inTransaction;
    private long generation;

    public RecoveringTransactionalProducer(Supplier<Producer<K, V>> producerSupplier, @Nullable String transactionalId) {
        this.producerSupplier = producerSupplier;
        this.transactionalId = transactionalId;
    }

    @Override
    public synchronized void initTransactions() {
        currentProducer();
    }

    @Override
    public synchronized void beginTransaction() {
        try {
            currentProducer().beginTransaction();
            inTransaction = true;
            pendingSends.clear();
            generation++;
        } catch (RuntimeException e) {
            if (!isTransactionalIdExpired(e)) {
                throw e;
            }
            replaceProducer();
            producer.beginTransaction();
            inTransaction = true;
            pendingSends.clear();
            generation++;
        }
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
        awaitPendingSends();
        synchronized (this) {
            try {
                currentProducer().sendOffsetsToTransaction(offsets, groupMetadata);
            } catch (RuntimeException e) {
                if (!recoverAndRetry(e, current -> current.sendOffsetsToTransaction(offsets, groupMetadata))) {
                    throw e;
                }
            }
        }
    }

    @Override
    public void commitTransaction() {
        awaitPendingSends();
        synchronized (this) {
            try {
                currentProducer().commitTransaction();
                completeTransaction();
            } catch (RuntimeException e) {
                if (recoverAndRetry(e, Producer::commitTransaction)) {
                    completeTransaction();
                } else {
                    throw e;
                }
            }
        }
    }

    @Override
    public synchronized void abortTransaction() {
        try {
            currentProducer().abortTransaction();
        } finally {
            completeTransaction();
        }
    }

    @Override
    public synchronized void registerMetricForSubscription(KafkaMetric metric) {
        currentProducer().registerMetricForSubscription(metric);
    }

    @Override
    public synchronized void unregisterMetricFromSubscription(KafkaMetric metric) {
        currentProducer().unregisterMetricFromSubscription(metric);
    }

    @Override
    public synchronized Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public synchronized Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        if (!inTransaction) {
            return currentProducer().send(record, callback);
        }
        PendingSend<K, V> pendingSend = new PendingSend<>(record, callback);
        pendingSends.add(pendingSend);
        dispatchSend(generation, pendingSend);
        return pendingSend.future();
    }

    @Override
    public synchronized void flush() {
        currentProducer().flush();
    }

    @Override
    public synchronized List<PartitionInfo> partitionsFor(String topic) {
        return currentProducer().partitionsFor(topic);
    }

    @Override
    public synchronized Map<MetricName, ? extends Metric> metrics() {
        return currentProducer().metrics();
    }

    @Override
    public synchronized Uuid clientInstanceId(Duration timeout) {
        return currentProducer().clientInstanceId(timeout);
    }

    @Override
    public synchronized void close() {
        close(Duration.ofSeconds(30));
    }

    @Override
    public synchronized void close(Duration timeout) {
        if (closed) {
            return;
        }
        closed = true;
        if (recoveryExecutor != null) {
            recoveryExecutor.shutdownNow();
        }
        closeProducer(producer, timeout);
        producer = null;
        pendingSends.clear();
        inTransaction = false;
    }

    private Producer<K, V> currentProducer() {
        if (closed) {
            throw new IllegalStateException("Producer is already closed");
        }
        if (producer == null) {
            producer = producerSupplier.get();
            producer.initTransactions();
        }
        return producer;
    }

    private void dispatchSend(long currentGeneration, PendingSend<K, V> pendingSend) {
        Producer<K, V> current = currentProducer();
        try {
            current.send(pendingSend.record(), (metadata, exception) -> handleSendResult(currentGeneration, pendingSend, metadata, exception));
        } catch (RuntimeException e) {
            handleSendFailure(currentGeneration, pendingSend, e);
        }
    }

    private void handleSendResult(long callbackGeneration, PendingSend<K, V> pendingSend, @Nullable RecordMetadata metadata, @Nullable Exception exception) {
        Callback callbackToInvoke = null;
        RecordMetadata callbackMetadata = metadata;
        Exception callbackException = exception;
        synchronized (this) {
            if (closed || callbackGeneration != generation || !inTransaction) {
                return;
            }
            if (exception != null && isTransactionalIdExpired(exception)) {
                long gen = generation;
                recoveryExecutor().execute(() -> recoverAndReplay(gen));
                return;
            }
            callbackToInvoke = pendingSend.complete(callbackMetadata, callbackException);
        }
        if (callbackToInvoke != null) {
            callbackToInvoke.onCompletion(callbackMetadata, callbackException);
        }
    }

    private void handleSendFailure(long callbackGeneration, PendingSend<K, V> pendingSend, RuntimeException exception) {
        Callback callbackToInvoke = null;
        synchronized (this) {
            if (closed || callbackGeneration != generation || !inTransaction) {
                return;
            }
            if (isTransactionalIdExpired(exception)) {
                long gen = generation;
                recoveryExecutor().execute(() -> recoverAndReplay(gen));
                return;
            }
            callbackToInvoke = pendingSend.complete(null, exception);
        }
        if (callbackToInvoke != null) {
            callbackToInvoke.onCompletion(null, exception);
        }
    }

    private boolean recoverAndRetry(RuntimeException exception, ProducerOperation<K, V> retry) {
        if (!isTransactionalIdExpired(exception)) {
            return false;
        }
        replayTransaction();
        retry.run(currentProducer());
        return true;
    }

    private void recoverAndReplay(long callbackGeneration) {
        List<FailedCallback<K, V>> failedCallbacks = new ArrayList<>();
        synchronized (this) {
            if (closed || callbackGeneration != generation || !inTransaction) {
                return;
            }
            try {
                replayTransaction();
            } catch (RuntimeException e) {
                LOG.warn("Failed to recover transactional producer [{}] after transactional id expiration: {}", transactionalId, e.getMessage(), e);
                for (PendingSend<K, V> ps : new ArrayList<>(pendingSends)) {
                    Callback callback = ps.complete(null, e);
                    if (callback != null) {
                        failedCallbacks.add(new FailedCallback<>(callback, e));
                    }
                }
                completeTransaction();
            }
        }
        for (FailedCallback<K, V> failedCallback : failedCallbacks) {
            failedCallback.callback().onCompletion(null, failedCallback.exception());
        }
    }

    private ExecutorService recoveryExecutor() {
        if (recoveryExecutor == null) {
            recoveryExecutor = Executors.newSingleThreadExecutor(r -> {
                var t = new Thread(r, "kafka-transactional-recovery");
                t.setDaemon(true);
                return t;
            });
        }
        return recoveryExecutor;
    }

    private void replayTransaction() {
        LOG.debug("Recreating transactional producer for transactional.id [{}] after Kafka expired the producer id", transactionalId);
        replaceProducer();
        generation++;
        producer.beginTransaction();
        for (PendingSend<K, V> pendingSend : pendingSends) {
            dispatchSend(generation, pendingSend);
        }
        inTransaction = true;
    }

    private void replaceProducer() {
        Producer<K, V> previous = producer;
        producer = producerSupplier.get();
        producer.initTransactions();
        closeProducer(previous, Duration.ZERO);
    }

    private void completeTransaction() {
        inTransaction = false;
        pendingSends.clear();
    }

    private void awaitPendingSends() {
        List<CompletableFuture<RecordMetadata>> futures;
        synchronized (this) {
            futures = pendingSends.stream()
                .map(PendingSend::future)
                .toList();
        }
        for (CompletableFuture<RecordMetadata> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KafkaException("Interrupted while awaiting transactional send completion", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                throw new KafkaException("Transactional send failed", cause);
            }
        }
    }

    private static boolean isTransactionalIdExpired(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof InvalidPidMappingException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static void closeProducer(@Nullable Producer<?, ?> producer, Duration timeout) {
        if (producer == null) {
            return;
        }
        try {
            producer.close(timeout);
        } catch (Exception e) {
            LOG.debug("Error closing expired transactional producer: {}", e.getMessage(), e);
        }
    }

    private record PendingSend<K, V>(ProducerRecord<K, V> record,
                                     @Nullable Callback callback,
                                     CompletableFuture<RecordMetadata> future) {
        private PendingSend(ProducerRecord<K, V> record, @Nullable Callback callback) {
            this(record, callback, new CompletableFuture<>());
        }

        private @Nullable Callback complete(@Nullable RecordMetadata metadata, @Nullable Exception exception) {
            if (future.isDone()) {
                return null;
            }
            if (exception == null) {
                future.complete(metadata);
            } else {
                future.completeExceptionally(exception);
            }
            return callback;
        }
    }

    private record FailedCallback<K, V>(Callback callback, Exception exception) {
    }

    @FunctionalInterface
    private interface ProducerOperation<K, V> {
        void run(Producer<K, V> producer);
    }
}
