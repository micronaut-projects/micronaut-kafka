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

import io.micronaut.configuration.kafka.KafkaAcknowledgement;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.core.annotation.Internal;
import org.jspecify.annotations.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.bind.DefaultExecutableBinder;
import io.micronaut.core.bind.ExecutableBinder;
import io.micronaut.core.util.CollectionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

/**
 * The internal state of the consumer in batch mode.
 *
 * @author Guillermo Calvo
 * @since 5.3
 */
@Internal
final class ConsumerStateBatch extends ConsumerState {

    ConsumerStateBatch(KafkaConsumerProcessor kafkaConsumerProcessor, ConsumerInfo info, Consumer<?, ?> consumer, Object consumerBean) {
        super(kafkaConsumerProcessor, info, consumer, consumerBean);
    }

    @Override
    @Nullable
    protected Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return info.errorStrategy.isRetry() ?
            kafkaConsumer.assignment().stream().collect(Collectors.toMap(identity(), this::getCurrentOffset)) : null;
    }

    @Override
    protected ConsumerRecords<?, ?> pollRecords(
        @Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Deserialization errors can happen while polling
        try {
            return kafkaConsumer.poll(info.pollTimeout);
        } catch (RecordDeserializationException ex) {
            // Try to honor the configured error strategy
            LOG.trace("Kafka consumer [{}] failed to deserialize value while polling", info.logMethod, ex);
            // By default, seek past the record to continue consumption
            kafkaConsumer.seek(ex.topicPartition(), ex.offset() + 1);
            // The error strategy and the exception handler can still decide what to do about this record
            resolveWithErrorStrategy(null, reconstructCurrentOffsetsIfAbsent(currentOffsets, ex), makeConsumerRecord(ex), ex);
            // By now, it's been decided whether this record should be retried and the exception may have been handled
            return null;
        }
    }

    @Override
    protected void processRecords(ConsumerRecords<?, ?> consumerRecords, @Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            // Bind Acknowledgement argument
            if (info.ackArg != null) {
                final Map<TopicPartition, OffsetAndMetadata> batchOffsets = getAckOffsets(consumerRecords);
                boundArguments.put(info.ackArg, (KafkaAcknowledgement) () -> kafkaConsumer.commitSync(batchOffsets));
            }
            final ExecutableBinder<ConsumerRecords<?, ?>> batchBinder = new DefaultExecutableBinder<>(boundArguments);
            final Object result = batchBinder.bind(info.method, kafkaConsumerProcessor.getBatchBinderRegistry(), consumerRecords).invoke(consumerBean);
            handleResult(normalizeResult(result), consumerRecords);
            failed = false;
        } catch (Exception e) {
            failed = resolveWithErrorStrategy(consumerRecords, currentOffsets, null, e);
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getAckOffsets(ConsumerRecords<?, ?> consumerRecords) {
        Map<TopicPartition, OffsetAndMetadata> ackOffsets = new HashMap<>();
        for (ConsumerRecord<?, ?> consumerRecord : consumerRecords) {
            final TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
            final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset() + 1, null);
            ackOffsets.put(topicPartition, offsetAndMetadata);
        }
        return ackOffsets;
    }

    @Nullable
    private static Object normalizeResult(@Nullable Object result) {
        if (result != null && result.getClass().isArray()) {
            return Arrays.asList((Object[]) result);
        }
        return result;
    }

    private void handleResult(Object result, ConsumerRecords<?, ?> consumerRecords) {
        if (result != null) {
            final boolean isPublisher = Publishers.isConvertibleToPublisher(result);
            final boolean isBlocking = info.isBlocking || !isPublisher;
            // Flux of tuples (consumer record / result)
            final Flux<? extends Tuple2<?, ? extends ConsumerRecord<?, ?>>> resultRecordFlux;
            // Flux of results
            final Flux<?> resultFlux;
            if (result instanceof Iterable<?> iterable) {
                resultFlux = Flux.fromIterable(iterable);
            } else if (isPublisher) {
                resultFlux = kafkaConsumerProcessor.convertPublisher(result);
            } else {
                resultFlux = Flux.just(result);
            }
            // Zip result flux with consumer records
            resultRecordFlux = resultFlux.zipWithIterable(consumerRecords)
                .doOnNext(x -> handleResultFlux(consumerRecords, x.getT2(), Flux.just(x.getT1()), isBlocking));
            // Block on the zipped flux or subscribe if non-blocking
            if (isBlocking) {
                resultRecordFlux.blockLast();
            } else {
                resultRecordFlux.subscribe();
            }
        }
    }

    @SuppressWarnings("java:S1874") // ErrorStrategyValue.NONE is deprecated
    private boolean resolveWithErrorStrategy(@Nullable ConsumerRecords<?, ?> consumerRecords,
        Map<TopicPartition, OffsetAndMetadata> currentOffsets, @Nullable ConsumerRecord<?, ?> consumerRecord, Throwable e) {
        if (info.errorStrategy.isRetry()) {
            final Set<TopicPartition> partitions = consumerRecords != null ? consumerRecords.partitions() : currentOffsets.keySet();
            if (shouldRetryException(e, consumerRecords, null) && info.retryCount > 0) {
                Map<TopicPartition, OffsetAndMetadata> reconstructedOffsets = reconstructCurrentOffsetsIfAbsent(currentOffsets, consumerRecords);
                // Check how many retries so far
                final int currentRetryCount = getCurrentRetryCount(partitions, reconstructedOffsets);
                if (info.retryCount >= currentRetryCount) {
                    // We will retry this batch again next time
                    if (info.shouldHandleAllExceptions) {
                        handleException(e, consumerRecords, null);
                    }
                    // Move back to the previous positions
                    partitions.forEach(tp -> kafkaConsumer.seek(tp, reconstructedOffsets.get(tp).offset()));
                    // Decide how long should we wait to retry this batch again
                    delayRetry(currentRetryCount, partitions);
                    return true;
                }
            }
            // We will NOT retry this batch anymore
            partitions.forEach(topicPartitionRetries::remove);
        }
        // Skip the failing batch of records
        publishToDlq(e, consumerRecords, consumerRecord);
        handleException(e, consumerRecords, consumerRecord);
        return info.errorStrategy == ErrorStrategyValue.NONE;
    }

    private int getCurrentRetryCount(Set<TopicPartition> partitions,
        @Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return partitions.stream()
            .map(tp -> {
                OffsetAndMetadata offsetAndMetadata = currentOffsets.get(tp);
                return offsetAndMetadata == null ? null : getPartitionRetryState(tp, offsetAndMetadata.offset());
            })
            .filter(Objects::nonNull)
            .mapToInt(x -> x.currentRetryCount)
            .max().orElse(info.retryCount);
    }

    private OffsetAndMetadata getCurrentOffset(TopicPartition tp) {
        return new OffsetAndMetadata(kafkaConsumer.position(tp), null);
    }

    private Map<TopicPartition, OffsetAndMetadata> reconstructCurrentOffsetsIfAbsent(
        @Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets, RecordDeserializationException ex) {
        // Current offsets can be missing after the first poll if assignments changed while records were being fetched.
        if (CollectionUtils.isEmpty(currentOffsets)) {
            return Map.of(ex.topicPartition(), new OffsetAndMetadata(ex.offset(), null));
        }
        if (currentOffsets.containsKey(ex.topicPartition())) {
            return currentOffsets;
        }
        Map<TopicPartition, OffsetAndMetadata> reconstructedOffsets = new HashMap<>(currentOffsets);
        reconstructedOffsets.put(ex.topicPartition(), new OffsetAndMetadata(ex.offset(), null));
        return reconstructedOffsets;
    }

    @Nullable
    private Map<TopicPartition, OffsetAndMetadata> reconstructCurrentOffsetsIfAbsent(
        @Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets,
        @Nullable ConsumerRecords<?, ?> consumerRecords) {
        // Current offsets can be missing for some partitions if assignments changed while records were being fetched.
        if (consumerRecords == null) {
            return currentOffsets;
        }
        Map<TopicPartition, OffsetAndMetadata> reconstructedOffsets = CollectionUtils.isEmpty(currentOffsets)
            ? new HashMap<>()
            : new HashMap<>(currentOffsets);
        boolean changed = CollectionUtils.isEmpty(currentOffsets);
        for (ConsumerRecord<?, ?> record : consumerRecords) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            if (!reconstructedOffsets.containsKey(tp)) {
                reconstructedOffsets.put(tp, new OffsetAndMetadata(record.offset(), null));
                changed = true;
            }
        }
        return changed ? reconstructedOffsets : currentOffsets;
    }

    private static ConsumerRecord<?, ?> makeConsumerRecord(RecordDeserializationException ex) {
        final TopicPartition tp = ex.topicPartition();
        return new ConsumerRecord<>(tp.topic(), tp.partition(), ex.offset(), null, null);
    }
}
