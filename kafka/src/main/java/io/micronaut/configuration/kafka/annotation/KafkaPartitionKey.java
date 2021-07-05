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
package io.micronaut.configuration.kafka.annotation;

import io.micronaut.core.bind.annotation.Bindable;

import java.lang.annotation.*;

/**
 * Parameter level annotation for Kafka producers to indicate which parameter to compute the Kafka Partition from.
 *
 * <p>The partition is computed by first serializing the object, using an appropriate serializer from
 * {@link io.micronaut.configuration.kafka.serde.SerdeRegistry} as determined by, and then computing the partition
 * number using the same algorithm as Kafka's own {@code DefaultStrategy} ({@code murmur2})<p/>
 *
 * <p>If the provided value is {@code null} then the configured/default partitioning strategy takes place.</p>
 *
 * <p>Note that while using {@link KafkaPartitionKey} in the same method as {@link KafkaPartition}
 * will not throw an exception, the outcome of doing so is left unspecified.</p>
 *
 * @author André Prata
 * @since 3.3.4
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
@Bindable
@Inherited
public @interface KafkaPartitionKey {
}
