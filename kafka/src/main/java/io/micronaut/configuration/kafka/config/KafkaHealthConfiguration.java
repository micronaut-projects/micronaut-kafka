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

import io.micronaut.core.util.Toggleable;

/**
 * Kafka Health indicator Configuration.
 * @since 5.1.0
 */
public interface KafkaHealthConfiguration extends Toggleable {
    /**
     * By default, the health check requires cluster-wide permissions in order to get information about the nodes in the Kafka cluster. If your application doesn't have admin privileges (for example, this might happen in multi-tenant scenarios), you can switch to a "restricted" version of the health check which only validates basic connectivity but doesn't require any additional permissions.
     * @return Whether to switch to a "restricted" version of the health check.
     */
    boolean isRestricted();
}
