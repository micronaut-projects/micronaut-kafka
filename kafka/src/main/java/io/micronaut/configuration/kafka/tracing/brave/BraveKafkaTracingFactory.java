/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.tracing.brave;

import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import brave.messaging.MessagingTracing;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;

import javax.annotation.Nullable;
import javax.inject.Singleton;

/**
 * Brave's Kafka tracing factory.
 *
 * @author dstepanov
 */
@Requires(classes = KafkaTracing.class)
@Requires(beans = Tracing.class)
@Factory
public class BraveKafkaTracingFactory {

    /**
     * Creates a default {@link KafkaTracing} using {@link MessagingTracing} or {@link Tracing}.
     *
     * @param tracing The {@link Tracing}
     * @param messagingTracing The {@link MessagingTracing} if exists
     * @return a new instance of {@link KafkaTracing}
     */
    @Requires(missingBeans = KafkaTracing.class)
    @Singleton
    KafkaTracing kafkaTracing(Tracing tracing, @Nullable MessagingTracing messagingTracing) {
      return messagingTracing == null ? KafkaTracing.create(tracing) : KafkaTracing.create(messagingTracing);
    }

}
