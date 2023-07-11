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
package io.micronaut.configuration.kafka.annotation;

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
     * The delay used with RETRY_ON_ERROR and RETRY_EXPONENTIALLY_ON_ERROR
     * {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * @return the delay by which to wait for the next retry
     */
    String retryDelay() default DEFAULT_DELAY_IN_SECONDS + "s";

    /**
     * The retry count used with RETRY_ON_ERROR and RETRY_EXPONENTIALLY_ON_ERROR
     * {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * @return the retry count of how many attempts should be made
     */
    int retryCount() default DEFAULT_RETRY_COUNT;

    /**
     * Whether all exceptions should be handled or ignored when using RETRY_ON_ERROR and RETRY_EXPONENTIALLY_ON_ERROR
     * {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * By default, only the last failed attempt will be handed over to the exception handler.
     *
     * @return whether all exceptions should be handled or ignored
     */
    boolean handleAllExceptions() default DEFAULT_HANDLE_ALL_EXCEPTIONS;

    /**
     * The strategy to use when an error occurs, see {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * @return the error strategy
     */
    ErrorStrategyValue value() default ErrorStrategyValue.NONE;

    /**
     * The types of exceptions to retry, used with RETRY_ON_ERROR, see {@link io.micronaut.configuration.kafka.annotation.ErrorStrategyValue}.
     *
     * @return the list of exceptions types
     * @since 4.5.0
     */
    Class<? extends Throwable>[] exceptionTypes() default {};
}
