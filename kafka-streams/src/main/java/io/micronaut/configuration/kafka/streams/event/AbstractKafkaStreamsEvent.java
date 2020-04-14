package io.micronaut.configuration.kafka.streams.event;

import io.micronaut.context.event.ApplicationEvent;
import org.apache.kafka.streams.KafkaStreams;

/**
 * Abstract class for kafka streams events.
 *
 * @author graemerocher
 * @since 2.0.0
 */
public abstract class AbstractKafkaStreamsEvent extends ApplicationEvent {
    private final KafkaStreams kafkaStreams;

    /**
     * Default constructor.
     * @param kafkaStreams The streams
     */
    protected AbstractKafkaStreamsEvent(KafkaStreams kafkaStreams) {
        super(kafkaStreams);
        this.kafkaStreams = kafkaStreams;
    }

    @Override
    public KafkaStreams getSource() {
        return (KafkaStreams) super.getSource();
    }

    /**
     * @return The kafka streams object.
     */
    public KafkaStreams getKafkaStreams() {
        return kafkaStreams;
    }
}
