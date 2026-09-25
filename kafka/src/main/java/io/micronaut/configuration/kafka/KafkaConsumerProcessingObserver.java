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

import io.micronaut.core.annotation.Internal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Internal integration point that lets an observability module observe the full processing
 * lifecycle of a single consumed {@link ConsumerRecord}, from the first delivery attempt through
 * to the record's terminal outcome (successful processing, or terminal failure once retries are
 * exhausted / the exception is not retryable).
 *
 * <p>The consumer processor invokes {@link #onStart} once when a record is first processed, keeps the
 * returned handle across application retries of that same record, and then invokes exactly one of
 * {@link #onSuccess} or {@link #onError} when the record reaches its terminal outcome. This is
 * deliberately different from {@link ConsumerRecordInterceptor}, which fires per delivery attempt
 * before listener binding and therefore cannot observe the terminal outcome of a record.</p>
 *
 * <p>This type is marked {@link Internal} on purpose: it exists to support the framework's own
 * Micrometer Observation integration and is not a stable, user-facing extension API. The signature
 * may change without notice. It intentionally exposes only Kafka client and primitive types so that
 * Micronaut Kafka does not gain a dependency on any observability library.</p>
 *
 * @author Sagar Kharab
 * @since 6.2.0
 */
@Internal
public interface KafkaConsumerProcessingObserver {

    /**
     * Invoked when a consumed record starts processing for the first time. On subsequent application
     * retries of the same record the processor reuses the handle returned here rather than starting
     * again, so implementations should treat this as "the record's processing has begun".
     *
     * @param record   the record about to be processed
     * @param clientId the Micronaut Kafka client id of the consumer
     * @param groupId  the Kafka consumer group id, if any
     * @return an opaque handle representing the started observation, or {@code null} to opt out of
     *     observing this record (in which case {@link #onSuccess}/{@link #onError} will not be called)
     */
    @Nullable
    Object onStart(@NonNull ConsumerRecord<?, ?> record, @NonNull String clientId, @Nullable String groupId);

    /**
     * Invoked once when the record has been processed successfully and will not be retried.
     *
     * @param handle the handle previously returned by {@link #onStart}
     */
    void onSuccess(@NonNull Object handle);

    /**
     * Invoked once when the record has reached a terminal failure and will not be retried (retries
     * exhausted or the exception is not retryable).
     *
     * @param handle the handle previously returned by {@link #onStart}
     * @param error  the terminal error
     */
    void onError(@NonNull Object handle, @NonNull Throwable error);
}
