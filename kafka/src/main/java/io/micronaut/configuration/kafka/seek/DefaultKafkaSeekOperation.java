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

import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

/**
 * Default implementation of {@link KafkaSeekOperation}.
 *
 * @param topicPartition the topic name and partition number on which the seek should be performed.
 * @param offsetType the offset type
 * @param offset the offset that should be used to perform the seek. Must be positive
 * @author Guillermo Calvo
 * @see KafkaSeekOperation
 * @since 4.1
 */
@Internal
record DefaultKafkaSeekOperation(
    @NonNull TopicPartition topicPartition, @NonNull OffsetType offsetType, long offset
) implements KafkaSeekOperation {

    /**
     * Creates a new instance.
     *
     * @param topicPartition the topic name and partition number on which the seek should be performed
     * @param offsetType     the offset type
     * @param offset         the offset that should be used to perform the seek. Must be positive
     */
    public DefaultKafkaSeekOperation {
        Objects.requireNonNull(topicPartition, "topicPartition");
        Objects.requireNonNull(topicPartition.topic(), "topicPartition.topic");
        Objects.requireNonNull(offsetType, "offsetType");
        if (offset < 0) {
            throw new IllegalArgumentException("Negative offset");
        }
    }
}
