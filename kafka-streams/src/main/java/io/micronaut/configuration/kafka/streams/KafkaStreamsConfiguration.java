package io.micronaut.configuration.kafka.streams;

import java.util.Properties;

/**
 * Interface used in {@link ConfiguredStreamBuilder}.
 *
 * @since 2.0
 * @author bobby
 */
public interface KafkaStreamsConfiguration {
    /**
     * The configuration.
     *
     * @return Properties for the streams configuration
     */
    Properties getConfig();
}
