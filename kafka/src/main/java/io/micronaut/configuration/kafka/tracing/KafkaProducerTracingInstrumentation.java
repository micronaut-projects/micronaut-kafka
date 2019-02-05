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

    public KafkaProducerTracingInstrumentation(Provider<Tracer> tracerProvider) {
        this.tracerProvider = tracerProvider;
    }

    @Override
    public Producer<?, ?> onCreated(BeanCreatedEvent<Producer<?, ?>> event) {
        final Producer<?, ?> kafkaProducer = event.getBean();
        return new TracingKafkaProducer<>(kafkaProducer, tracerProvider.get());
    }
}
