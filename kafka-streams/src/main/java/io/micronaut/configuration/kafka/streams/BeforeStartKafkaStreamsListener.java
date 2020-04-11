package io.micronaut.configuration.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Listener to execute before KafkaStreams.start() in {@link KafkaStreamsFactory#kafkaStreams}
 *
 * @author henriquelsm
 * @since 1.5.0
 */
public interface BeforeStartKafkaStreamsListener {

    /**
     * @param kafkaStreams a instance of current kafkaStreams.
     * @param kStreams array with all KStream of the kafkaStreams
     */
    void execute(KafkaStreams kafkaStreams, KStream[] kStreams);
}
