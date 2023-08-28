/*
 * Copyright 2017-2023 original authors
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
package io.micronaut.configuration.kafka.config;

import io.micronaut.context.annotation.ConfigurationProperties;

/**
 * {@link ConfigurationProperties} implementation of {@link DefaultKafkaListenerExceptionHandlerConfiguration}.
 * @since 5.1.0
 */
@ConfigurationProperties(DefaultKafkaListenerExceptionHandlerConfigurationProperties.PREFIX)
public class DefaultKafkaListenerExceptionHandlerConfigurationProperties implements DefaultKafkaListenerExceptionHandlerConfiguration {

    /**
     * The default prefix used for the default Kafka listener exception handler configuration.
     */
    public static final String PREFIX = AbstractKafkaConfiguration.PREFIX + ".default-listener-exception-handler";

    /**
     * The default value for {@code skipRecordOnDeserializationFailure}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final boolean DEFAULT_SKIP_RECORD_ON_DESERIALIZATION_FAILURE = true;

    /**
     * The default value for {@code commitRecordOnDeserializationFailure}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final boolean DEFAULT_COMMIT_RECORD_ON_DESERIALIZATION_FAILURE = false;

    private boolean skipRecordOnDeserializationFailure = DEFAULT_SKIP_RECORD_ON_DESERIALIZATION_FAILURE;

    private boolean commitRecordOnDeserializationFailure = DEFAULT_COMMIT_RECORD_ON_DESERIALIZATION_FAILURE;

    @Override
    public boolean isSkipRecordOnDeserializationFailure() {
        return skipRecordOnDeserializationFailure;
    }

    /**
     * Whether to skip record on deserialization failure. Default value {@value #DEFAULT_SKIP_RECORD_ON_DESERIALIZATION_FAILURE}
     *
     * @param skipRecordOnDeserializationFailure Whether to skip record on deserialization failure.
     */
    public void setSkipRecordOnDeserializationFailure(boolean skipRecordOnDeserializationFailure) {
        this.skipRecordOnDeserializationFailure = skipRecordOnDeserializationFailure;
    }

    @Override
    public boolean isCommitRecordOnDeserializationFailure() {
        return commitRecordOnDeserializationFailure;
    }

    /**
     * Whether to commit record on deserialization failure. Default value {@value #DEFAULT_COMMIT_RECORD_ON_DESERIALIZATION_FAILURE}
     *
     * @param commitRecordOnDeserializationFailure Whether to commit record on deserialization failure.
     */
    public void setCommitRecordOnDeserializationFailure(boolean commitRecordOnDeserializationFailure) {
        this.commitRecordOnDeserializationFailure = commitRecordOnDeserializationFailure;
    }
}
