/*
 * Copyright 2017-2020 original authors
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
package io.micronaut.configuration.kafka.admin;

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;

import jakarta.inject.Singleton;

/**
 * Creates the Kafka {@link org.apache.kafka.clients.admin.AdminClient}.
 *
 * <p>Micronaut Kafka looks up the admin client as an {@link org.apache.kafka.clients.admin.Admin} bean,
 * for topic creation, the health indicator and consumer group deletion. To use another {@code Admin}
 * implementation, such as the Oracle OKafka admin client, replace this bean or set
 * {@code kafka.admin.enabled=false}. Registering a second {@code Admin} bean next to this one makes
 * those lookups fail with a {@link io.micronaut.context.exceptions.NonUniqueBeanException}.</p>
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Factory
@Requires(beans = KafkaDefaultConfiguration.class)
@Requires(property = AbstractKafkaConfiguration.PREFIX + ".admin.enabled", notEquals = StringUtils.FALSE, defaultValue = StringUtils.TRUE)
public class AdminClientFactory {

    /**
     * Creates the admin client.
     *
     * @param configuration The configuration to use.
     * @return The admin client
     */
    @Bean(preDestroy = "close")
    @Singleton
    AdminClient adminClient(KafkaDefaultConfiguration configuration) {
        return AdminClient.create(configuration.getConfig());
    }

}
