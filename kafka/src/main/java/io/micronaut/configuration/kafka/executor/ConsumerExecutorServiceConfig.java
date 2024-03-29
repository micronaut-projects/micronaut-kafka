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
package io.micronaut.configuration.kafka.executor;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.executor.ExecutorConfiguration;
import io.micronaut.scheduling.executor.ExecutorType;
import io.micronaut.scheduling.executor.UserExecutorConfiguration;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

/**
 * Configures a {@link java.util.concurrent.ScheduledExecutorService} for running {@link io.micronaut.configuration.kafka.annotation.KafkaListener} instances.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Requires(missingProperty = ExecutorConfiguration.PREFIX_CONSUMER)
@Factory
public class ConsumerExecutorServiceConfig {


    /**
     * @return The executor configurations
     */
    @Singleton
    @Named(TaskExecutors.MESSAGE_CONSUMER)
    ExecutorConfiguration configuration() {
        return UserExecutorConfiguration.of(ExecutorType.SCHEDULED, 75);
    }
}
