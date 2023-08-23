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
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Performs {@link KafkaSeekOperation seek operations} on a target consumer.
 *
 * @author Guillermo Calvo
 * @see KafkaSeekOperation
 * @since 4.1
 */
public interface KafkaSeeker {

    /**
     * Creates a new {@link KafkaSeeker} with a given target {@link Consumer}.
     *
     * @param consumer the target consumer.
     * @return a new kafka seeker.
     */
    @Internal
    @NonNull
    static KafkaSeeker newInstance(@NonNull Consumer<?, ?> consumer) {
        return new DefaultKafkaSeeker(consumer);
    }

    /**
     * Performs a kafka seek operation immediately.
     *
     * @param operation the kafka seek operation to perform.
     * @return whether the seek operation succeeded or not.
     */
    boolean perform(@NonNull KafkaSeekOperation operation);
}
