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
package io.micronaut.configuration.kafka.processor;

import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.ShutdownEvent;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stops the {@link io.micronaut.configuration.kafka.annotation.KafkaListener} consumers when the
 * {@link ShutdownEvent} is published, before any singleton is destroyed.
 *
 * <p>Without this, the consumers are only stopped when {@link KafkaConsumerProcessor} is destroyed,
 * and a bean that a listener uses (for example a {@code DataSource}) may be destroyed first, since the
 * processor has no dependency on it. Stopping here waits for the records being processed, so they
 * finish against live dependencies.</p>
 *
 * <p>This runs after graceful shutdown (when it is enabled), which has already drained the
 * consumers within its grace period. In that case this only stops what graceful shutdown did not.</p>
 *
 * <p>The consumers stay registered, so that {@link io.micronaut.configuration.kafka.ConsumerRegistry} can
 * still look them up by ID in later shutdown listeners. They are removed when the processor is destroyed.</p>
 *
 * <p>Only a processor that has already been created is stopped: listening to the event must not create
 * one during shutdown.</p>
 *
 * @since 6.2.1
 */
@Singleton
@Requires(beans = KafkaDefaultConfiguration.class)
@Internal
final class KafkaConsumerShutdownListener implements ApplicationEventListener<ShutdownEvent>, Ordered {

    /**
     * Runs right after {@code io.micronaut.runtime.graceful.GracefulShutdownListener}, which has
     * {@link Ordered#HIGHEST_PRECEDENCE}, and before other shutdown listeners.
     */
    static final int ORDER = Ordered.HIGHEST_PRECEDENCE + 100;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerShutdownListener.class);

    private final BeanContext beanContext;

    KafkaConsumerShutdownListener(BeanContext beanContext) {
        this.beanContext = beanContext;
    }

    @Override
    public void onApplicationEvent(ShutdownEvent event) {
        for (BeanRegistration<KafkaConsumerProcessor> registration : beanContext.getActiveBeanRegistrations(KafkaConsumerProcessor.class)) {
            try {
                registration.bean().stopConsumers();
            } catch (Exception e) {
                LOG.error("Error stopping Kafka consumers on shutdown", e);
            }
        }
    }

    @Override
    public int getOrder() {
        return ORDER;
    }
}
