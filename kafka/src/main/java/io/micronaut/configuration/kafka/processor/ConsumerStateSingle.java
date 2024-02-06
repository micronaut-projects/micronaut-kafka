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
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.seek.KafkaSeekOperations;
import io.micronaut.configuration.kafka.seek.KafkaSeeker;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.bind.DefaultExecutableBinder;
import io.micronaut.core.bind.ExecutableBinder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import reactor.core.publisher.Flux;

import java.util.*;

/**
 * The internal state of the consumer in single mode.
 *
 * @author Guillermo Calvo
 * @since 5.3
 */
@Internal
final class ConsumerStateSingle extends ConsumerState {

    ConsumerStateSingle(KafkaConsumerProcessor kafkaConsumerProcessor, ConsumerInfo info, Consumer<?, ?> consumer, Object consumerBean) {
        super(kafkaConsumerProcessor, info, consumer, consumerBean);
    }

    @Override
    @Nullable
    protected Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return info.trackPartitions ? new HashMap<>() : null;
    }

    @Override
    protected ConsumerRecords<?, ?> pollRecords(@Nullable Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Deserialization errors can happen while polling
        try {
            return kafkaConsumer.poll(info.pollTimeout);
        } catch (RecordDeserializationException ex) {
            // Try to honor the configured error strategy
            LOG.trace("Kafka consumer [{}] failed to deserialize value while polling", info.logMethod, ex);
            // By default, seek past the record to continue consumption
            kafkaConsumer.seek(ex.topicPartition(), ex.offset() + 1);
            // The error strategy and the exception handler can still decide what to do about this record
            resolveWithErrorStrategy(null, makeConsumerRecord(ex), ex);
            // By now, it's been decided whether this record should be retried and the exception may have been handled
            return null;
        }
    }

    @Override
    protected void processRecords(ConsumerRecords<?, ?> consumerRecords,
        Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        final Iterator<? extends ConsumerRecord<?, ?>> iterator = consumerRecords.iterator();
        while (iterator.hasNext()) {
            final ConsumerRecord<?, ?> consumerRecord = iterator.next();

            LOG.trace("Kafka consumer [{}] received record: {}", info.logMethod, consumerRecord);

            if (info.trackPartitions) {
                final TopicPartition topicPartition = getTopicPartition(consumerRecord);
                final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset() + 1, null);
                currentOffsets.put(topicPartition, offsetAndMetadata);
            }

            final KafkaSeekOperations seek = Optional.ofNullable(info.seekArg).map(x -> KafkaSeekOperations.newInstance()).orElse(null);
            Optional.ofNullable(info.seekArg).ifPresent(argument -> boundArguments.put(argument, seek));
            Optional.ofNullable(info.ackArg).ifPresent(argument -> boundArguments.put(argument, (KafkaAcknowledgement) () -> kafkaConsumer.commitSync(currentOffsets)));

            try {
                process(consumerRecord, consumerRecords);
            } catch (Exception e) {
                if (resolveWithErrorStrategy(consumerRecords, consumerRecord, e)) {
                    resetTheFollowingPartitions(consumerRecord, iterator);
                    failed = true;
                    return;
                }
            }

            if (info.offsetStrategy == OffsetStrategy.SYNC_PER_RECORD) {
                commitSync(consumerRecords, consumerRecord, currentOffsets);
            } else if (info.offsetStrategy == OffsetStrategy.ASYNC_PER_RECORD) {
                kafkaConsumer.commitAsync(currentOffsets, this::resolveCommitCallback);
            }

            if (seek != null) {
                // Performs seek operations that were deferred by the user
                final KafkaSeeker seeker = KafkaSeeker.newInstance(kafkaConsumer);
                seek.forEach(seeker::perform);
            }
        }
        failed = false;
    }

    private void process(ConsumerRecord<?, ?> consumerRecord,
        ConsumerRecords<?, ?> consumerRecords) {
        final ExecutableBinder<ConsumerRecord<?, ?>> executableBinder = new DefaultExecutableBinder<>(boundArguments);
        final Object result = executableBinder.bind(info.method, kafkaConsumerProcessor.getBinderRegistry(), consumerRecord).invoke(consumerBean);
        if (result != null) {
            final boolean isPublisher = Publishers.isConvertibleToPublisher(result);
            final Flux<?> publisher = isPublisher ? kafkaConsumerProcessor.convertPublisher(result) : Flux.just(result);
            handleResultFlux(consumerRecords, consumerRecord, publisher, isPublisher || info.isBlocking);
        }
    }

    @SuppressWarnings("java:S1874") // ErrorStrategyValue.NONE is deprecated
    private boolean resolveWithErrorStrategy(@Nullable ConsumerRecords<?, ?> consumerRecords,
        ConsumerRecord<?, ?> consumerRecord, Throwable e) {
        if (info.errorStrategy.isRetry()) {
            final TopicPartition topicPartition = getTopicPartition(consumerRecord);
            if (shouldRetryException(e, consumerRecords, consumerRecord) && info.retryCount > 0) {
                // Check how many retries so far
                final int currentRetryCount = getCurrentRetryCount(consumerRecord);
                if (info.retryCount >= currentRetryCount) {
                    // We will retry this batch again next time
                    if (info.shouldHandleAllExceptions) {
                        handleException(e, consumerRecords, consumerRecord);
                    }
                    // Move back to the previous position
                    kafkaConsumer.seek(topicPartition, consumerRecord.offset());
                    // Decide how long should we wait to retry this batch again
                    delayRetry(currentRetryCount, Collections.singleton(topicPartition));
                    return true;
                }
            }
            // We will NOT retry this record anymore
            topicPartitionRetries.remove(topicPartition);
        }
        // Skip the failing record
        handleException(e, consumerRecords, consumerRecord);
        return info.errorStrategy == ErrorStrategyValue.NONE;
    }

    private void commitSync(ConsumerRecords<?, ?> consumerRecords, ConsumerRecord<?, ?> consumerRecord, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            kafkaConsumer.commitSync(currentOffsets);
        } catch (CommitFailedException e) {
            handleException(e, consumerRecords, consumerRecord);
        }
    }

    private void resolveCommitCallback(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (consumerBean instanceof OffsetCommitCallback occ) {
            occ.onComplete(offsets, exception);
        } else if (exception != null) {
            LOG.error("Error asynchronously committing Kafka offsets [{}]: {}", offsets,
                exception.getMessage(), exception);
        }
    }

    private void resetTheFollowingPartitions(ConsumerRecord<?, ?> errorConsumerRecord, Iterator<? extends ConsumerRecord<?, ?>> iterator) {
        Set<Integer> processedPartition = new HashSet<>();
        processedPartition.add(errorConsumerRecord.partition());
        while (iterator.hasNext()) {
            ConsumerRecord<?, ?> consumerRecord = iterator.next();
            if (!processedPartition.add(consumerRecord.partition())) {
                continue;
            }
            kafkaConsumer.seek(getTopicPartition(consumerRecord), consumerRecord.offset());
        }
    }

    private int getCurrentRetryCount(ConsumerRecord<?, ?> consumerRecord) {
        return getPartitionRetryState(getTopicPartition(consumerRecord), consumerRecord.offset()).currentRetryCount;
    }

    private static TopicPartition getTopicPartition(ConsumerRecord<?, ?> consumerRecord) {
        return new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
    }

    private static ConsumerRecord<?, ?> makeConsumerRecord(RecordDeserializationException ex) {
        final TopicPartition tp = ex.topicPartition();
        return new ConsumerRecord<>(tp.topic(), tp.partition(), ex.offset(), null, null);
    }
}
