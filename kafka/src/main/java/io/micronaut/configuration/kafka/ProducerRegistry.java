package io.micronaut.configuration.kafka;

import io.micronaut.core.type.Argument;
import org.apache.kafka.clients.producer.Producer;

import javax.annotation.Nonnull;

/**
 * A registry of managed {@link Producer} instances key by id and type.
 *
 * @author graemerocher
 * @since 1.0
 */
public interface ProducerRegistry {
    /**
     * Returns a managed Producer.
     *
     * @param id The id of the producer.
     * @param keyType The key type
     * @param valueType The value type
     * @param <K> The key generic type
     * @param <V> The value generic type
     * @return The producer
     */
    @Nonnull
    <K, V> Producer<K, V> getProducer(String id, Argument<K> keyType, Argument<V> valueType);
}
