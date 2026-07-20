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
package io.micronaut.configuration.kafka.annotation;

import io.micronaut.context.annotation.AliasFor;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Setting the error strategy allows you to resume at the next offset
 * or to seek the consumer (stop on error) to the failed offset so that
 * it can retry if an error occurs.
 *
 * The consumer bean is still able to implement a custom exception handler to replace
 * {@link io.micronaut.configuration.kafka.exceptions.DefaultKafkaListenerExceptionHandler}
 * as well as set the error strategy.
 *
 * @since 4.1
 * @author Christopher Webb
 * @author Vishal Sulibhavi
 * @author Denis Stepanov
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface ErrorStrategy {

    /**
     * Default retry delay in seconds.
     */
    int DEFAULT_DELAY_IN_SECONDS = 1;

    /**
     * Default retry attempts.
     */
    int DEFAULT_RETRY_COUNT = 1;

    /**
     * Default handle all exceptions.
     */
    boolean DEFAULT_HANDLE_ALL_EXCEPTIONS = false;

    /**
     * The delay used with RETRY_ON_ERROR, RETRY_EXPONENTIALLY_ON_ERROR,
     * RETRY_CONDITIONALLY_ON_ERROR and RETRY_CONDITIONALLY_EXPONENTIALLY_ON_ERROR
     * {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * @return the delay by which to wait for the next retry
     */
    String retryDelay() default DEFAULT_DELAY_IN_SECONDS + "s";

    /**
     * The fixed retry count used with RETRY_ON_ERROR and RETRY_EXPONENTIALLY_ON_ERROR,
     * RETRY_CONDITIONALLY_ON_ERROR and RETRY_CONDITIONALLY_EXPONENTIALLY_ON_ERROR
     * {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * <p>{@code retryCount} takes precedence over {@code retryCountValue} if they are both set.
     *
     * @return the retry count of how many attempts should be made
     * @see ErrorStrategy#retryCountValue()
     */
    int retryCount() default DEFAULT_RETRY_COUNT;

    /**
     * The dynamic retry count used with RETRY_ON_ERROR and RETRY_EXPONENTIALLY_ON_ERROR
     * {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * <p>{@code retryCountValue} will be overridden by {@code retryCount} if they are both set.
     *
     * @return the retry count of how many attempts should be made
     * @see ErrorStrategy#retryCount()
     */
    @AliasFor(member = "retryCount")
    String retryCountValue() default "";

    /**
     * Whether all exceptions should be handled or ignored when using RETRY_ON_ERROR and RETRY_EXPONENTIALLY_ON_ERROR
     * {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * By default, only the last failed attempt will be handed over to the exception handler.
     *
     * @return whether all exceptions should be handled or ignored
     * @since 5.0
     */
    boolean handleAllExceptions() default DEFAULT_HANDLE_ALL_EXCEPTIONS;

    /**
     * Whether Micronaut should pause the affected topic partitions after the last retryable failure
     * instead of skipping past the failed record.
     *
     * <p>When enabled, the consumer seeks back to the failed offset, handles the exception, and
     * pauses the affected partitions until they are resumed through the {@code ConsumerRegistry}
     * or the application is restarted.
     *
     * @return whether to stop consuming from the affected partitions after retries are exhausted
     * @since 6.0
     */
    boolean stopOnExhaustedRetry() default false;

    /**
     * The strategy to use when an error occurs, see {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * @return the error strategy
     */
    ErrorStrategyValue value() default ErrorStrategyValue.NONE;

    /**
     * The dead letter topic to publish failed records to when using
     * {@link ErrorStrategyValue#LOG_AND_RESUME_AT_NEXT_RECORD} or
     * {@link ErrorStrategyValue#RETRY_TOPIC_ON_ERROR} after retry topics are exhausted.
     *
     * @return The dead letter topic name
     * @since 5.8
     */
    String dlq() default "";

    /**
     * The suffixes used to derive retry topics from the original topic name when using
     * {@link ErrorStrategyValue#RETRY_TOPIC_ON_ERROR}.
     *
     * <p>For example, with an original topic {@code orders} and suffixes
     * {@code -retry-5s} and {@code -retry-30s}, Micronaut will use the retry topics
     * {@code orders-retry-5s} and {@code orders-retry-30s}.</p>
     *
     * @return The retry topic suffixes
     * @since 6.0.0
     */
    String[] retryTopicSuffixes() default {};

    /**
     * The delays to apply before consuming records from the retry topics declared in
     * {@link #retryTopicSuffixes()} when using
     * {@link ErrorStrategyValue#RETRY_TOPIC_ON_ERROR}.
     *
     * <p>The number of configured delays must match the number of configured retry topic suffixes.</p>
     *
     * @return The retry topic delays
     * @since 6.0.0
     */
    String[] retryTopicDelays() default {};

    /**
     * The types of exceptions to retry, used with RETRY_ON_ERROR and RETRY_EXPONENTIALLY_ON_ERROR,
     * see {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     * When used with RETRY_CONDITIONALLY_ON_ERROR and RETRY_CONDITIONALLY_EXPONENTIALLY_ON_ERROR,
     * the skip behaviour will be overridden if the thrown exception is one of these types.
     *
     * @return the list of exceptions types
     * @since 4.5.0
     */
    Class<? extends Throwable>[] exceptionTypes() default {};
}
