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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Default implementation of {@link KafkaSeekOperations}.
 *
 * @author Guillermo Calvo
 * @see KafkaSeekOperations
 * @since 4.1
 */
@Internal
final class DefaultKafkaSeekOperations implements KafkaSeekOperations {

    private final List<KafkaSeekOperation> operations = new ArrayList<>();

    @Override
    @NonNull
    public Iterator<KafkaSeekOperation> iterator() {
        return operations.iterator();
    }

    @Override
    public void defer(@NonNull KafkaSeekOperation operation) {
        Objects.requireNonNull(operation, "operation");
        operations.add(operation);
    }
}
