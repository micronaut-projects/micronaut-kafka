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
 * {@link ConfigurationProperties} implementation of {@link KafkaHealthConfiguration}.
 */
@ConfigurationProperties(KafkaHealthConfigurationProperties.PREFIX)
public class KafkaHealthConfigurationProperties implements KafkaHealthConfiguration {

    /**
     * The default prefix used for Kafka Health configuration.
     */
    public static final String PREFIX = AbstractKafkaConfiguration.PREFIX + ".health";

    /**
     * The default enable value.
     */
    @SuppressWarnings("WeakerAccess")
    public static final boolean DEFAULT_ENABLED = true;

    /**
     * The default restricted value.
     */
    @SuppressWarnings("WeakerAccess")
    public static final boolean DEFAULT_RESTRICTED = false;

    private boolean enabled = DEFAULT_ENABLED;

    private boolean restricted = DEFAULT_RESTRICTED;

    @Override
    public boolean isEnabled() {
        return this.enabled;
    }

    /**
     * Whether the Kafka health check is enabled. Default value {@value #DEFAULT_ENABLED}.
     *
     * @param enabled Whether the Kafka health check is enabled.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public boolean isRestricted() {
        return restricted;
    }

    /**
     * By default, the health check requires cluster-wide permissions in order to get information about the nodes in the Kafka cluster. If your application doesn't have admin privileges (for example, this might happen in multi-tenant scenarios), you can switch to a "restricted" version of the health check which only validates basic connectivity but doesn't require any additional permissions.. Default value {@value #DEFAULT_RESTRICTED}
     *
     * @param restricted Whether to switch to a "restricted" version of the health check.
     */
    public void setRestricted(boolean restricted) {
        this.restricted = restricted;
    }
}
