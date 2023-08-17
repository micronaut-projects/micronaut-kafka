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

import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.configuration.kafka.annotation.KafkaListener;

/**
 * Defines an interface that can be injected into {@link KafkaListener} beans so that
 * {@link KafkaSeekOperation seek operations} can be eventually performed on a consumer.
 *
 * <p>The operations will be performed by Micronaut automatically, when the consumer method
 * completes successfully, possibly after committing offsets via {@link OffsetStrategy#AUTO}.</p>
 *
 * @author Guillermo Calvo
 * @see KafkaSeekOperation
 * @since 4.1
 */
public interface KafkaSeekOperations extends Iterable<KafkaSeekOperation>, KafkaSeekOperation.Builder {

    /**
     * Creates a new {@link KafkaSeekOperations} instance.
     *
     * @return a new instance.
     */
    @Internal
    @NonNull
    static KafkaSeekOperations newInstance() {
        return new DefaultKafkaSeekOperations();
    }

    /**
     * Adds a {@link KafkaSeekOperation} to the list.
     *
     * @param operation the kafka seek operation to eventually perform.
     */
    void defer(@NonNull KafkaSeekOperation operation);
}
