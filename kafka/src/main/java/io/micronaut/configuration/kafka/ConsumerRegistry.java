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
package io.micronaut.configuration.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import io.micronaut.core.annotation.NonNull;

import java.util.Collection;
import java.util.Set;

/**
 * A registry for created Kafka consumers. Also provides methods for pausing and resuming consumers.
 *
 * @author graemerocher
 * @since 1.1
 */
public interface ConsumerRegistry {

    /**
     * Returns a managed Consumer. Note that the consumer should not be interacted with directly from a
     * different thread to the poll loop!
     *
     * @param id The id of the producer.
     * @param <K> The key generic type
     * @param <V> The value generic type
     * @return The consumer
     * @throws IllegalArgumentException If no consumer exists for the given ID
     */
    @NonNull
    <K, V> Consumer<K, V> getConsumer(@NonNull String id);

    /**
     * Returns a managed Consumer's subscriptions.
     *
     * @param id The id of the producer.
     * @return The consumer subscription
     * @throws IllegalArgumentException If no consumer exists for the given ID
     */
    @NonNull
    Set<String> getConsumerSubscription(@NonNull String id);

    /**
     * Returns a managed Consumer's assignment info.
     *
     * @param id The id of the producer.
     * @return The consumer assignment
     * @throws IllegalArgumentException If no consumer exists for the given ID
     */
    @NonNull
    Set<TopicPartition> getConsumerAssignment(@NonNull String id);

    /**
     * The IDs of the available consumers.
     *
     * @return The consumers
     */
    @NonNull Set<String> getConsumerIds();

    /**
     * Is the consumer with the given ID paused.
     *
     * @param id the consumers id
     * @return True if it is paused
     */
    boolean isPaused(@NonNull String id);

    /**
     * Is the consumer with the given ID paused to consume from the given topic partitions.
     *
     * @param id the consumers id
     * @return True if it is paused for the topic partitions
     * @since 4.0.1
     */
    boolean isPaused(@NonNull String id, @NonNull Collection<TopicPartition> topicPartitions);

    /**
     * Pause the consumer for the given ID. Note that this method will request that the consumer is paused, however
     * does not block until the consumer is actually paused. You can use the {@link #isPaused(String)} method to
     * establish when the consumer has actually been paused.
     *
     * @param id The id of the consumer
     */
    void pause(@NonNull String id);

    /**
     * Pause the consumer for the given ID to consume from the given topic partitions.
     * Note that this method will request that the consumer is paused, however
     * does not block until the consumer is actually paused.
     * You can use the {@link #isPaused(String, Collection<TopicPartition>)} method to
     * establish when the consumer has actually been paused for the topic partitions.
     *
     * @param id The id of the consumer
     * @param topicPartitions The topic partitions to pause consuming from
     * @since 4.0.1
     */
    void pause(@NonNull String id, @NonNull Collection<TopicPartition> topicPartitions);


    /**
     * Resume the consumer for the given ID. Note that this method will request that the consumer is resumed, however
     * does not block until the consumer is actually resumed. You can use the {@link #isPaused(String)} method to
     * establish when the consumer has actually been resumed.
     *
     * @param id The id of the consumer
     */
    void resume(@NonNull String id);

    /**
     * Resume the consumer for the given ID to consume from the given topic partitions.
     * Note that this method will request that the consumer is resumed, however
     * does not block until the consumer is actually resumed.
     * You can use the {@link #isPaused(String, Collection<TopicPartition>)} method to
     * establish when the consumer has actually been resumed to consume from the given topic partitions.
     *
     * @param id The id of the consumer
     * @param topicPartitions The topic partitions to pause consuming from
     * @since 4.0.1
     */
    void resume(@NonNull String id, @NonNull Collection<TopicPartition> topicPartitions);


}
