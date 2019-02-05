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
package io.micronaut.configuration.kafka.tracing;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Instruments Kafka producers with Open Tracing support.
 *
 * @author graemerocher
 * @since 1.1
 */
@Singleton
@Requires(beans = Tracer.class)
@Requires(classes = TracingKafkaProducer.class)
public class KafkaProducerTracingInstrumentation implements BeanCreatedEventListener<Producer<?, ?>> {

    private final Provider<Tracer> tracerProvider;

    /**
     * Default constructor.
     * @param tracerProvider The tracer provider
     */
    protected KafkaProducerTracingInstrumentation(Provider<Tracer> tracerProvider) {
        this.tracerProvider = tracerProvider;
    }

    @Override
    public Producer<?, ?> onCreated(BeanCreatedEvent<Producer<?, ?>> event) {
        final Producer<?, ?> kafkaProducer = event.getBean();
        return new TracingKafkaProducer<>(kafkaProducer, tracerProvider.get());
    }
}
