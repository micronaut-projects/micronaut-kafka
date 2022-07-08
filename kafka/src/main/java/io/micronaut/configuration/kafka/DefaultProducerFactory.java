package io.micronaut.configuration.kafka;

import java.util.Properties;

import io.micronaut.core.annotation.NonNull;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A default implementation of {@link ProducerFactory} used for creating producer.
 *
 * @author milanspre
 * @since 5.0.0
 */
public class DefaultProducerFactory implements ProducerFactory {

    /**
     *
     * Creates kafka producer, could be overridden for further control.
     *
     * @param config properties for producer
     * @param ks key serializer
     * @param vs value serializer
     * @param <K> key type
     * @param <V> value type
     * @since 5.0.0
     * @return new instance of producer
     */
    @Override
    @NonNull
    public <K, V> Producer<K, V> createProducer(Properties config, Serializer<K> ks, Serializer<V> vs) {
        return new KafkaProducer<>(config, ks, vs);
    }
}
