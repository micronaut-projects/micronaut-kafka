package io.micronaut.configuration.kafka.streams.listeners;

import io.micronaut.configuration.kafka.streams.BeforeStartKafkaStreamsListener;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;

import javax.inject.Singleton;

@Singleton
public class BeforeStartKafkaStreamsListenerImp implements BeforeStartKafkaStreamsListener {
    private boolean executed = false;

    @Override
    public void execute(KafkaStreams kafkaStreams, KStream[] kStreams) {
        executed = true;
    }

    public boolean isExecuted() {
        return executed;
    }
}
