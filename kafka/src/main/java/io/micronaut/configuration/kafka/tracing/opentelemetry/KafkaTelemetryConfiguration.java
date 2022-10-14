/*
 * Copyright 2017-2022 original authors
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
package io.micronaut.configuration.kafka.tracing.opentelemetry;

import java.util.Collections;
import java.util.Set;

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;

/**
 * Configuration properties for KafkaTelemetry.
 *
 * @since 4.5.0
 */
@Requires(property = KafkaTelemetryConfiguration.PREFIX + ".enabled", notEquals = StringUtils.FALSE)
@ConfigurationProperties(KafkaTelemetryConfiguration.PREFIX)
public class KafkaTelemetryConfiguration {

    /**
     * The default prefix used for Kafka Telemetry configuration.
     */
    public static final String PREFIX = AbstractKafkaConfiguration.PREFIX + ".otel";
    /**
     * Value to capture all headers as span attributes.
     */
    public static final String ALL_HEADERS = "*";
    /**
     * If you want to set headers as lists, set "true".
     */
    private boolean headersAsLists;
    /**
     * List of headers, which you want to add as span attributes. By default, all headers
     * will be added as span attributes. If you don't want to set any headers as attributes,
     * just set it to `null` or an empty string.
     */
    private Set<String> capturedHeaders = Collections.singleton(ALL_HEADERS);

    /**
     * If you want to set headers as lists, set "true".
     *
     * @return headersAsLists flag
     */
    public boolean isHeadersAsLists() {
        return headersAsLists;
    }

    /**
     * Setter for headersAsLists flag.
     *
     * @param headersAsLists flag
     */
    public void setHeadersAsLists(boolean headersAsLists) {
        this.headersAsLists = headersAsLists;
    }

    /**
     * List of headers, which you want to add as span attributes. By default, all headers
     * will be added as span attributes. If you don't want to set any headers as attributes,
     * just set it to `null` or an empty string.
     *
     * @return capturedHeaders set
     */
    public Set<String> getCapturedHeaders() {
        return capturedHeaders;
    }

    /**
     * Setter for captured headers set.
     *
     * @param capturedHeaders capturedHeaders set
     */
    public void setCapturedHeaders(Set<String> capturedHeaders) {
        this.capturedHeaders = capturedHeaders;
    }
}
