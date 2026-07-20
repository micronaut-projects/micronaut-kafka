/*
 * Copyright 2017-2026 original authors
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

import io.micronaut.core.order.Ordered;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Intercepts a consumed Kafka {@link ConsumerRecord} before Micronaut binds it to a listener method.
 * Implementations may return the original record, return a wrapped record that preserves the consumed
 * record coordinates, or return {@code null} to skip listener invocation for the consumed record.
 *
 * <p>When Micronaut manages offset commits, returning {@code null} still allows the framework to advance
 * offsets for the consumed record according to the configured listener offset strategy.</p>
 *
 * <p>Wrapped records must preserve the original topic, partition, and offset because Micronaut continues
 * to use the consumed record coordinates for listener routing, retries, and offset management.</p>
 *
 * @param <K> The key type
 * @param <V> The value type
 * @author Graeme Rocher
 * @since 6.0.0
 */
public interface ConsumerRecordInterceptor<K, V> extends Ordered {

    /**
     * Determine whether this interceptor applies to the given listener method.
     *
     * @param beanDefinition The listener bean definition
     * @param method The executable listener method
     * @return {@code true} if this interceptor should be applied
     */
    default boolean matches(@NonNull BeanDefinition<?> beanDefinition, @NonNull ExecutableMethod<?, ?> method) {
        return true;
    }

    /**
     * Intercept the consumed record before Micronaut binds it to a listener method.
     *
     * @param interceptionContext The interception context
     * @return The record to bind, or {@code null} to skip listener invocation
     */
    @Nullable
    ConsumerRecord<K, V> intercept(@NonNull InterceptionContext<K, V> interceptionContext);

    /**
     * The context for record interception.
     *
     * @param <K> The key type
     * @param <V> The value type
     * @param consumerRecord The consumed record
     * @param topic The topic being processed
     * @param partition The partition being processed
     * @param offset The offset being processed
     * @param clientId The Micronaut Kafka client id
     * @param groupId The Kafka consumer group id
     */
    record InterceptionContext<K, V>(
        @NonNull ConsumerRecord<K, V> consumerRecord,
        @NonNull String topic,
        int partition,
        long offset,
        @NonNull String clientId,
        @Nullable String groupId
    ) {
    }
}
