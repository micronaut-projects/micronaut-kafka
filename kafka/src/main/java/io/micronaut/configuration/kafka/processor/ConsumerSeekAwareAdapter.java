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

import io.micronaut.configuration.kafka.ConsumerSeekAware;
import io.micronaut.configuration.kafka.seek.KafkaSeeker;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;

/**
 * Internal adapter for {@link ConsumerSeekAware}.
 *
 * @param seeker The {@link KafkaSeeker} to use.
 * @param bean The {@link ConsumerSeekAware} to invoke
 * @author Guillermo Calvo
 * @since 5.2
 */
@Internal
record ConsumerSeekAwareAdapter(@NonNull KafkaSeeker seeker, @NonNull ConsumerSeekAware bean)
    implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(@Nullable Collection<TopicPartition> partitions) {
        bean.onPartitionsRevoked(partitions != null ? partitions : Collections.emptyList());
    }

    @Override
    public void onPartitionsAssigned(@Nullable Collection<TopicPartition> partitions) {
        bean.onPartitionsAssigned(partitions != null ? partitions : Collections.emptyList(), seeker);
    }
}
