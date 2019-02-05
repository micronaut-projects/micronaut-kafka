package io.micronaut.configuration.kafka.tracing;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;

import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Instruments Kafka consumers with Open Tracing support.
 *
 * @author graemerocher
 * @since 1.1
 */
@Singleton
@Requires(beans = Tracer.class)
@Requires(classes = TracingKafkaConsumer.class)
public class KafkaConsumerTracingInstrumentation implements BeanCreatedEventListener<Consumer<?, ?>> {

    private final Provider<Tracer> tracerProvider;

    public KafkaConsumerTracingInstrumentation(Provider<Tracer> tracerProvider) {
        this.tracerProvider = tracerProvider;
    }

    @Override
    public Consumer<?, ?> onCreated(BeanCreatedEvent<Consumer<?, ?>> event) {
        final Consumer<?, ?> consumer = event.getBean();
        return new TracingKafkaConsumer<>(consumer, tracerProvider.get());
    }
}
