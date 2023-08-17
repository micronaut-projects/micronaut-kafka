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
package io.micronaut.configuration.kafka.seek;

import io.micronaut.core.annotation.NonNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * Represents a {@code seek} operation that may be performed on a {@link Consumer}.
 *
 * <p>If successful, the operation will determine the next offset returned by
 * {@link Consumer#poll(Duration)}.</p>
 *
 * @author Guillermo Calvo
 * @see Consumer#seek(TopicPartition, long)
 * @see Consumer#poll(Duration)
 * @since 4.1
 */
public interface KafkaSeekOperation {

    /**
     * @return the topic name and partition number on which the seek should be performed.
     */
    @NonNull
    TopicPartition topicPartition();

    /**
     * @return the offset type.
     */
    @NonNull
    OffsetType offsetType();

    /**
     * @return the offset that should be used to perform the seek. Must be positive.
     */
    long offset();

    /**
     * @return the topic name on which the seek will be performed.
     */
    @NonNull
    default String topic() {
        return topicPartition().topic();
    }

    /**
     * @return the partition number on which the seek will be performed.
     */
    default int partition() {
        return topicPartition().partition();
    }

    /**
     * Determines the interpretation of the {@link KafkaSeekOperation#offset()} value.
     */
    enum OffsetType {

        /** The offset is absolute. */
        ABSOLUTE,

        /** The offset goes forward, relative to the current position. */
        FORWARD,

        /** The offset goes backward, relative to the current position. */
        BACKWARD,

        /** The offset goes forward, relative to the beginning of the partition. */
        BEGINNING,

        /** The offset goes backward, relative to the end of the partition. */
        END,

        /** The offset represents a Kafka timestamp. */
        TIMESTAMP,
    }

    /**
     * Convenience trait interface that can be used to create new {@link KafkaSeekOperation} instances.
     */
    interface Builder {

        /**
         * Creates an absolute seek operation.
         *
         * @param topicPartition the topic partition.
         * @param offset         the absolute offset. Must be zero or greater.
         * @return an absolute seek operation.
         */
        @NonNull
        default KafkaSeekOperation seek(@NonNull TopicPartition topicPartition, long offset) {
            return new DefaultKafkaSeekOperation(topicPartition, OffsetType.ABSOLUTE, offset);
        }

        /**
         * Creates a seek operation relative to the beginning.
         *
         * @param topicPartition the topic partition.
         * @param offset         the offset. Must be zero or greater.
         * @return a seek operation relative to the beginning.
         */
        @NonNull
        default KafkaSeekOperation seekRelativeToBeginning(@NonNull TopicPartition topicPartition, long offset) {
            return new DefaultKafkaSeekOperation(topicPartition, OffsetType.BEGINNING, offset);
        }

        /**
         * Creates a seek to the beginning operation.
         *
         * @param topicPartition the topic partition.
         * @return a seek to the beginning operation.
         */
        @NonNull
        default KafkaSeekOperation seekToBeginning(@NonNull TopicPartition topicPartition) {
            return new DefaultKafkaSeekOperation(topicPartition, OffsetType.BEGINNING, 0L);
        }

        /**
         * Creates a list of seek to the beginning operations.
         *
         * @param partitions the {@link TopicPartition}s.
         * @return a list of seek to the beginning operations.
         */
        @NonNull
        default List<KafkaSeekOperation> seekToBeginning(@NonNull Collection<TopicPartition> partitions) {
            return partitions.stream().map(this::seekToBeginning).toList();
        }

        /**
         * Creates a seek operation relative to the end.
         *
         * @param topicPartition the topic partition.
         * @param offset         the offset. Must be zero or greater.
         * @return a seek operation relative to the end.
         */
        @NonNull
        default KafkaSeekOperation seekRelativeToEnd(@NonNull TopicPartition topicPartition, long offset) {
            return new DefaultKafkaSeekOperation(topicPartition, OffsetType.END, offset);
        }

        /**
         * Creates a seek to the end operation.
         *
         * @param topicPartition the topic partition.
         * @return a seek to the end operation.
         */
        @NonNull
        default KafkaSeekOperation seekToEnd(@NonNull TopicPartition topicPartition) {
            return new DefaultKafkaSeekOperation(topicPartition, OffsetType.END, 0L);
        }

        /**
         * Creates a list of seek to the end operations.
         *
         * @param partitions the {@link TopicPartition}s.
         * @return a list of seek to the end operations.
         */
        @NonNull
        default List<KafkaSeekOperation> seekToEnd(@NonNull Collection<TopicPartition> partitions) {
            return partitions.stream().map(this::seekToEnd).toList();
        }

        /**
         * Creates a forward seek operation.
         *
         * @param topicPartition the topic partition.
         * @param offset         the offset. Must be zero or greater.
         * @return a forward seek operation.
         */
        @NonNull
        default KafkaSeekOperation seekForward(@NonNull TopicPartition topicPartition, long offset) {
            return new DefaultKafkaSeekOperation(topicPartition, OffsetType.FORWARD, offset);
        }

        /**
         * Creates a backward seek operation.
         *
         * @param topicPartition the topic partition.
         * @param offset         the offset. Must be zero or greater.
         * @return a backward seek operation.
         */
        @NonNull
        default KafkaSeekOperation seekBackward(@NonNull TopicPartition topicPartition, long offset) {
            return new DefaultKafkaSeekOperation(topicPartition, OffsetType.BACKWARD, offset);
        }

        /**
         * Creates a seek to the timestamp operation.
         *
         * <p>This operation will seek to the first offset whose timestamp is greater than or equal to
         * the given one if it exists; otherwise it will seek to the end.</p>
         *
         * @param topicPartition the topic partition.
         * @param timestamp      the kafka time stamp.
         * @return a seek to the timestamp operation.
         */
        @NonNull
        default KafkaSeekOperation seekToTimestamp(@NonNull TopicPartition topicPartition, long timestamp) {
            return new DefaultKafkaSeekOperation(topicPartition, OffsetType.TIMESTAMP, timestamp);
        }

        /**
         * Creates a list of seek to the timestamp operations.
         *
         * <p>This operation will seek to the first offset whose timestamp is greater than or equal to
         * the given one if it exists; otherwise it will seek to the end.</p>
         *
         * @param topicPartitions the topic/partitions.
         * @param timestamp       the kafka time stamp.
         * @return a list of seek to the timestamp operations.
         */
        @NonNull
        default List<KafkaSeekOperation> seekToTimestamp(@NonNull Collection<TopicPartition> topicPartitions, long timestamp) {
            return topicPartitions.stream().map(tp -> seekToTimestamp(tp, timestamp)).toList();
        }
    }
}
