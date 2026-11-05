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
 * Internal integration point that lets an observability module observe the processing lifecycle of a
 * single consumed {@link ConsumerRecord}, from the first delivery attempt through to the record's
 * terminal outcome (successful processing, or terminal failure once retries are exhausted / the
 * exception is not retryable).
 *
 * <p>The consumer processor invokes {@link #onStart} when it starts handling a record and keeps the
 * returned handle across blocking retries of that same Kafka record. Around every individual processing
 * attempt (the first delivery and each blocking retry) it brackets the listener invocation and any
 * handling of its failure with {@link #onAttemptStart} and {@link #onAttemptEnd}, allowing the
 * observation to be made the current context for that attempt only and to be cleared again before any
 * retry delay. Each observation then ends with exactly one of {@link #onSuccess} or {@link #onError},
 * after the attempt has ended. This is deliberately different from {@link ConsumerRecordInterceptor},
 * which fires per delivery attempt before listener binding and therefore cannot observe the terminal
 * outcome of a record.</p>
 *
 * <p>A successful non-blocking retry-topic dispatch is treated as the terminal outcome of the source
 * record's delivery ({@link #onError}): the failed record has been handed off to a separate retry
 * topic and, when that retry record is later consumed, it begins its own observation. In other words a
 * single observation spans all <em>blocking</em> attempts of one consumed Kafka record, not a
 * distributed retry-topic pipeline.</p>
 *
 * <p>Only listeners that consume single records are observed: batch listeners receive no callbacks.
 * Failures to publish a listener result to its {@code @SendTo} destination are reported only if they are
 * known when the listener invocation returns; a failure the producer reports later is not.</p>
 *
 * <p>All callbacks are invoked fail-open by the processor: an exception or linkage error thrown by any
 * method here is caught and logged, and never alters Kafka delivery semantics (listener invocation,
 * retries, offset commits, or dead-letter publishing).</p>
 *
 * <p>This type is marked {@link Internal} on purpose: it exists to support the framework's own
 * Micrometer Observation integration and is not a stable, user-facing extension API. The signature
 * may change without notice. It intentionally exposes only Kafka client and primitive types so that
 * Micronaut Kafka does not gain a dependency on any observability library.</p>
 *
 * @author Sagar Kharab
 * @since 6.3.0
 */
@Internal
public interface KafkaConsumerProcessingObserver {

    /**
     * Invoked when a new observation of a consumed record starts. On subsequent blocking retries of the
     * same record the processor reuses the handle returned here rather than starting again, so
     * implementations should treat this as "the record's processing has begun". The opt-out decision (a
     * {@code null} return) is remembered in the same way.
     *
     * <p>This is called once per observation, which is not always once per record: a record that is
     * delivered again after its observation ended, for example when a partition paused by
     * {@code stopOnExhaustedRetry} is resumed or a revoked partition is assigned back, starts a new one.</p>
     *
     * <p>A record that failed to deserialize while polling is passed with a {@code null} key and value
     * and no headers.</p>
     *
     * @param record   the record about to be processed
     * @param clientId the Micronaut Kafka client id of the consumer
     * @param groupId  the Kafka consumer group id, if any
     * @return an opaque handle representing the started observation, or {@code null} to opt out of
     *     observing this record (in which case no further callbacks will be invoked for it)
     */
    @Nullable
    Object onStart(@NonNull ConsumerRecord<?, ?> record, @NonNull String clientId, @Nullable String groupId);

    /**
     * Invoked immediately before each individual processing attempt of the record (the first delivery
     * and every blocking retry), after {@link #onStart}. Implementations typically make the observation
     * the current context here so that listener work and downstream instrumentation observe it. The
     * returned token is passed back to {@link #onAttemptEnd} once the attempt completes.
     *
     * @param handle the handle previously returned by {@link #onStart}
     * @return an opaque per-attempt token (for example a scope to close), or {@code null} if there is
     *     nothing to close at the end of the attempt
     */
    @Nullable
    Object onAttemptStart(@NonNull Object handle);

    /**
     * Invoked once per processing attempt for which {@link #onAttemptStart} returned a token, after the
     * listener invocation and the handling of any failure (exception handler, dead-letter or retry-topic
     * publishing), so that any per-attempt context opened by {@link #onAttemptStart} is cleared before a
     * retry delay. It does not signal the record's terminal outcome.
     *
     * @param attempt the token previously returned by {@link #onAttemptStart}
     */
    void onAttemptEnd(@NonNull Object attempt);

    /**
     * Invoked once when the record has been processed successfully and will not be retried.
     *
     * @param handle the handle previously returned by {@link #onStart}
     */
    void onSuccess(@NonNull Object handle);

    /**
     * Invoked once when the observation of a record ends in failure: retries exhausted, the retry strategy
     * stopped on the paused partition, the exception is not retryable, the failed record was handed off to
     * a non-blocking retry topic, or the listener result could not be published to its destination. If the
     * record is delivered again later, that starts a new observation (see {@link #onStart}).
     *
     * <p>It is also invoked, with a
     * {@link io.micronaut.configuration.kafka.exceptions.KafkaListenerException}, when this consumer
     * abandons a record before its terminal outcome: the partition was revoked, the consumer stopped, the
     * partition moved to another offset, or an interceptor skipped the record on a retry.</p>
     *
     * @param handle the handle previously returned by {@link #onStart}
     * @param error  the terminal error
     */
    void onError(@NonNull Object handle, @NonNull Throwable error);
}
