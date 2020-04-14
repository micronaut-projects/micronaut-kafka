package io.micronaut.configuration.kafka.streams.event;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;

/**
 * An event fired before a {@link KafkaStreams} object starts.
 *
 * @author graemerocher
 * @since 2.0.0
 */
public class BeforeKafkaStreamStart extends AbstractKafkaStreamsEvent {
    private final KStream<?, ?>[] streams;

    /**
     * Default constructor.
     * @param kafkaStreams The kafka streams object
     * @param streams The kstreams
     */
    public BeforeKafkaStreamStart(KafkaStreams kafkaStreams, KStream<?, ?>[] streams) {
        super(kafkaStreams);
        this.streams = streams;
    }

    /**
     * @return The streams
     */
    public KStream<?, ?>[] getStreams() {
        return streams;
    }
}
