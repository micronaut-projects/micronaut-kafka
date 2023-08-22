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

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.seek.KafkaSeekOperation;
import io.micronaut.configuration.kafka.seek.KafkaSeeker;
import io.micronaut.core.annotation.NonNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;

/**
 * Interface for {@link KafkaListener} instances to implement if they wish to perform
 * {@link KafkaSeekOperation seek operations} when the set of partitions assigned to the
 * {@link Consumer} changes.
 *
 * <p>This callback interface is based on {@link ConsumerRebalanceListener} and it provides a
 * {@link KafkaSeeker} object that can perform {@link KafkaSeekOperation} instances immediately.</p>
 *
 * @author Guillermo Calvo
 * @see ConsumerRebalanceListener
 * @see KafkaSeekOperation
 * @see KafkaSeeker
 * @since 4.1
 */
@FunctionalInterface
public interface ConsumerSeekAware {

    /**
     * A callback method the user can implement to provide handling of customized offsets
     * on completion of a successful partition re-assignment.
     *
     * <p>This method will be called after the partition re-assignment completes and before the
     * consumer starts fetching data, and only as the result of a
     * {@link Consumer#poll(Duration) poll(long)} call.
     * Under normal conditions, {@link #onPartitionsRevoked(Collection)} will be executed before
     * {@link #onPartitionsAssigned(Collection, KafkaSeeker)}.</p>
     *
     * <p>The provided {@link KafkaSeeker} object can perform
     * {@link KafkaSeekOperation seek operations} on the underlying consumer.</p>
     *
     * @param partitions The list of partitions that are now assigned to the consumer
     * @param seeker     The object that can perform {@link KafkaSeekOperation seek operations}
     * @see ConsumerRebalanceListener#onPartitionsAssigned(Collection)
     */
    void onPartitionsAssigned(@NonNull Collection<TopicPartition> partitions, @NonNull KafkaSeeker seeker);

    /**
     * @see ConsumerRebalanceListener#onPartitionsRevoked(Collection)
     * @param partitions The list of partitions that were assigned to the consumer and now need to
     *                   be revoked (may not include all currently assigned partitions, i.e. there
     *                   may still be some partitions left)
     */
    default void onPartitionsRevoked(@NonNull Collection<TopicPartition> partitions) { }

    /**
     * @see ConsumerRebalanceListener#onPartitionsLost(Collection)
     * @param partitions The list of partitions that are now assigned to the consumer (previously
     *                   owned partitions will NOT be included, i.e. this list will only include
     *                   newly added partitions)
     */
    default void onPartitionsLost(@NonNull Collection<TopicPartition> partitions) {
        onPartitionsRevoked(partitions);
    }
}
