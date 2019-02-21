package io.micronaut.configuration.kafka;

import io.micronaut.messaging.Acknowledgement;

/**
 * Defines an interface that can be injected into {@link io.micronaut.configuration.kafka.annotation.KafkaListener} beans so that offsets can be manually committed.
 *
 * @author James Kleeh
 * @since 1.1.0
 */
public interface KafkaAcknowledgement extends Acknowledgement, io.micronaut.configuration.kafka.Acknowledgement {

    /**
     * Kafka does not support rejection of messages explicitly.
     *
     * @throws UnsupportedOperationException if called
     */
    @Override
    default void nack() {
        throw new UnsupportedOperationException("Kafka does not support rejection of messages explicitly");
    }
}
