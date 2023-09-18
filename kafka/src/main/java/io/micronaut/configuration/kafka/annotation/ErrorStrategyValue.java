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

import java.time.Duration;

/**
 * Defines the type of error handling strategy that micronaut-kafka will perform in case
 * of error. The default exception handler or any custom exception handler will be performed
 * after the error handling strategy has been run.
 *
 * @author Christopher Webb
 * @author Vishal Sulibhavi
 * @since 4.1
 */
public enum ErrorStrategyValue {
    /**
     * This strategy will stop consuming subsequent records in the case of an error and will
     * attempt to re-consume the current record indefinitely.
     */
    RETRY_ON_ERROR,

    /**
     * This strategy will stop consuming subsequent records in the case of an error and will
     * attempt to re-consume the current record with exponentially growing time breaks between
     * consumption attempts. Breaks' duration is computed based on the n * 2^(k - 1) formula,
     * where n is the initial delay, and k is the number of retries.
     */
    RETRY_EXPONENTIALLY_ON_ERROR,

    /**
     * This strategy will ignore the current error and will resume at the next offset.
     */
    RESUME_AT_NEXT_RECORD,

    /**
     * This error strategy will skip over all records from the current offset in
     * the current poll when the consumer encounters an error.
     *
     * @deprecated maintain broken, but consistent behaviour with previous versions of micronaut-kafka that
     * do not support error strategy.
     *
     * See https://github.com/micronaut-projects/micronaut-kafka/issues/372
     */
    @Deprecated
    NONE;

    /**
     *
     * @return Whether this is a retry error strategy.
     */
    public boolean isRetry() {
        return this == RETRY_ON_ERROR || this == RETRY_EXPONENTIALLY_ON_ERROR;
    }

    /**
     * Compute retry delay given a fixed delay and the number of attempts.
     *
     * @param fixedRetryDelay The fixed retry delay.
     * @param retryAttempts The number of retries so far.
     * @return The amount of time to wait before trying again.
     */
    public Duration computeRetryDelay(Duration fixedRetryDelay, long retryAttempts) {
        if (!isRetry()) {
            return Duration.ZERO;
        }
        final Duration delay = fixedRetryDelay != null ? fixedRetryDelay : Duration.ofSeconds(ErrorStrategy.DEFAULT_DELAY_IN_SECONDS);
        if (this == ErrorStrategyValue.RETRY_EXPONENTIALLY_ON_ERROR) {
            return delay.multipliedBy(1L << (retryAttempts - 1));
        }
        return delay;
    }
}
