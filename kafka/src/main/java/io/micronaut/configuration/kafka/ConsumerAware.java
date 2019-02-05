package io.micronaut.configuration.kafka;

import org.apache.kafka.clients.consumer.Consumer;

import javax.annotation.Nonnull;

/**
 * Interface for {@link io.micronaut.configuration.kafka.annotation.KafkaListener} instances to implement
 * if they wish to obtain a reference to the underlying {@link Consumer}.
 *
 * @param <K> The key type
 * @param <V> The value type
 * @author Graeme Rocher
 * @since 1.0
 */
public interface ConsumerAware<K, V> {
    /**
     * Called when the underlying {@link Consumer} is created.
     *
     * @param consumer The consumer
     */
    void setKafkaConsumer(@Nonnull Consumer<K, V> consumer);
}
