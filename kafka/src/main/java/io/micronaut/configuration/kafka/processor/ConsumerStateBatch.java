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

import io.micronaut.configuration.kafka.KafkaAcknowledgement;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.bind.DefaultExecutableBinder;
import io.micronaut.core.bind.ExecutableBinder;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.inject.ExecutableMethod;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
        try {
            return kafkaConsumer.poll(info.pollTimeout);
        } catch (RecordDeserializationException ex) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Kafka consumer [{}] failed to deserialize value while polling", info.logMethod(ex.topicPartition().topic()), ex);
            }
            if (info.offsetStrategy != OffsetStrategy.DISABLED) {
                kafkaConsumer.seek(ex.topicPartition(), ex.offset() + 1);
            }
            resolveWithErrorStrategy(null, reconstructCurrentOffsetsIfAbsent(currentOffsets, ex), makeConsumerRecord(ex), ex);
            return null;
        }
    }

    @Override
    protected void processRecords(ConsumerRecords<?, ?> consumerRecords, @Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            final ConsumerRecords<?, ?> interceptedConsumerRecords = kafkaConsumerProcessor.interceptRecords(info, consumerRecords);
            if (interceptedConsumerRecords.isEmpty()) {
                final String topic = consumerRecords.partitions().stream().findFirst().map(TopicPartition::topic).orElseThrow();
                handleResult(normalizeResult(null), consumerRecords, topic);
                failed = false;
                return;
            }
            for (ConsumerRecords<?, ?> topicRecords : recordsByTopic(interceptedConsumerRecords)) {
                withKafkaScope(() -> {
                    final String topic = topicRecords.partitions().stream().findFirst().map(TopicPartition::topic).orElseThrow();
                    final ExecutableMethod<Object, ?> method = info.methodForTopic(topic);
                    Optional.ofNullable(info.ackArg(topic)).ifPresent(argument -> {
                        final Map<TopicPartition, OffsetAndMetadata> batchOffsets = getAckOffsets(topicRecords);
                        boundArguments.put(argument, (KafkaAcknowledgement) () -> kafkaConsumer.commitSync(batchOffsets));
                    });
                    Optional.ofNullable(info.consumerArg(topic)).ifPresent(argument -> boundArguments.put(argument, kafkaConsumer));
                    if (method.isSuspend()) {
                        Argument<?> lastArgument = method.getArguments()[method.getArguments().length - 1];
                        boundArguments.put(lastArgument, null);
                    }
                    final ExecutableBinder<ConsumerRecords<?, ?>> batchBinder = new DefaultExecutableBinder<>(boundArguments);
                    final Object result = batchBinder.bind(method, kafkaConsumerProcessor.getBatchBinderRegistry(), topicRecords).invoke(consumerBean);
                    handleResult(normalizeResult(result), topicRecords, topic);
                });
            }
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

    private void handleResult(Object result, ConsumerRecords<?, ?> consumerRecords, String topic) {
        if (result != null) {
            final boolean isPublisher = Publishers.isConvertibleToPublisher(result);
            final boolean isBlocking = info.isBlocking(topic) || !isPublisher;
            final Flux<? extends Tuple2<?, ? extends ConsumerRecord<?, ?>>> resultRecordFlux;
            final Flux<?> resultFlux;
            if (result instanceof Iterable<?> iterable) {
                resultFlux = Flux.fromIterable(iterable);
            } else if (isPublisher) {
                resultFlux = kafkaConsumerProcessor.convertPublisher(result);
            } else {
                resultFlux = Flux.just(result);
            }
            resultRecordFlux = resultFlux.zipWithIterable(consumerRecords)
                .doOnNext(x -> handleResultFlux(consumerRecords, x.getT2(), topic, Flux.just(x.getT1()), isBlocking));
            if (isBlocking) {
                resultRecordFlux.blockLast();
            } else {
                resultRecordFlux.subscribe();
            }
        }
    }

    @SuppressWarnings("java:S1874") // ErrorStrategyValue.NONE is deprecated
    private boolean resolveWithErrorStrategy(
        @Nullable ConsumerRecords<?, ?> consumerRecords,
        Map<TopicPartition, OffsetAndMetadata> currentOffsets,
        @Nullable ConsumerRecord<?, ?> consumerRecord,
        Throwable e
    ) {
        if (info.errorStrategy.isRetry()) {
            final Set<TopicPartition> partitions = consumerRecords != null ? consumerRecords.partitions() : currentOffsets.keySet();
            if (shouldRetryException(e, consumerRecords, null) && info.retryCount > 0) {
                Map<TopicPartition, OffsetAndMetadata> reconstructedOffsets = reconstructCurrentOffsetsIfAbsent(currentOffsets, consumerRecords);
                final int currentRetryCount = getCurrentRetryCount(partitions, reconstructedOffsets);
                if (info.retryCount >= currentRetryCount) {
                    if (info.shouldHandleAllExceptions) {
                        handleException(e, consumerRecords, null);
                    }
                    partitions.forEach(tp -> kafkaConsumer.seek(tp, reconstructedOffsets.get(tp).offset()));
                    delayRetry(currentRetryCount, partitions);
                    return true;
                }
            }
            partitions.forEach(topicPartitionRetries::remove);
        }
        publishToDlq(e, consumerRecords, consumerRecord);
        handleException(e, consumerRecords, consumerRecord);
        return info.errorStrategy == ErrorStrategyValue.NONE;
    }

    private int getCurrentRetryCount(Set<TopicPartition> partitions, @Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
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
        @Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets,
        RecordDeserializationException ex
    ) {
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
        @Nullable ConsumerRecords<?, ?> consumerRecords
    ) {
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

    private java.util.List<ConsumerRecords<?, ?>> recordsByTopic(ConsumerRecords<?, ?> consumerRecords) {
        if (!info.routesByTopic()) {
            return java.util.List.of(consumerRecords);
        }
        return splitByTopic(consumerRecords);
    }

    private static java.util.List<ConsumerRecords<?, ?>> splitByTopic(ConsumerRecords<?, ?> consumerRecords) {
        if (consumerRecords.partitions().stream().map(TopicPartition::topic).distinct().count() <= 1) {
            return java.util.List.of(consumerRecords);
        }
        Map<String, Map<TopicPartition, java.util.List<ConsumerRecord<?, ?>>>> byTopic = new LinkedHashMap<>();
        for (ConsumerRecord<?, ?> consumerRecord : consumerRecords) {
            byTopic.computeIfAbsent(consumerRecord.topic(), ignored -> new LinkedHashMap<>())
                .computeIfAbsent(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), ignored -> new ArrayList<>())
                .add(consumerRecord);
        }
        java.util.List<ConsumerRecords<?, ?>> splitRecords = new ArrayList<>(byTopic.size());
        for (Map<TopicPartition, java.util.List<ConsumerRecord<?, ?>>> topicRecords : byTopic.values()) {
            splitRecords.add(new ConsumerRecords<>((Map) topicRecords, Collections.emptyMap()));
        }
        return splitRecords;
    }
}
