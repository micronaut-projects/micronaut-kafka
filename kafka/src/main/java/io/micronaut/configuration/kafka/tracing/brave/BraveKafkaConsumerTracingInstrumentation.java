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
package io.micronaut.configuration.kafka.tracing.brave;

import brave.kafka.clients.KafkaTracing;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import org.apache.kafka.clients.consumer.Consumer;

import javax.inject.Singleton;

/**
 * Kafka consumer tracing instrumentation using Brave.
 *
 * @author dstepanov
 */
@Singleton
@Requires(beans = KafkaTracing.class)
public class BraveKafkaConsumerTracingInstrumentation implements BeanCreatedEventListener<Consumer<?, ?>> {

    private final KafkaTracing kafkaTracing;

    /**
     * Default constructor.
     * @param kafkaTracing The kafka tracing
     */
    public BraveKafkaConsumerTracingInstrumentation(KafkaTracing kafkaTracing) {
        this.kafkaTracing = kafkaTracing;
    }

    @Override
    public Consumer<?, ?> onCreated(BeanCreatedEvent<Consumer<?, ?>> event) {
        return kafkaTracing.consumer(event.getBean());
    }
}
