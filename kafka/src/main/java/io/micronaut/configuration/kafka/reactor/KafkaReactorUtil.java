/*
 * Copyright 2017-2021 original authors
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
package io.micronaut.configuration.kafka.reactor;

import io.micronaut.core.annotation.Internal;
import org.apache.kafka.common.KafkaFuture;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/**
 * Utility methods for working with Kafka and Reactor.
 *
 * @author graemerocher
 * @since 4.0.0
 */
@Internal
public class KafkaReactorUtil {

    /**
     * Factory for creating Mono instances from Kafka Future instances.
     * @param supplier The supplier
     * @param <T> The type
     * @return The Mono
     */
    public static <T> Mono<T> fromKafkaFuture(Supplier<KafkaFuture<T>> supplier) {
        return Mono.create((sink) -> supplier.get().whenComplete((result, error) -> {
            if (error != null) {
                sink.error(error);
            } else if (result != null) {
                sink.success(result);
            } else {
                sink.success();
            }
        }));
    }
}
