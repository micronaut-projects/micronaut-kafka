package io.micronaut.configuration.kafka.streams.startkafkastreams;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Factory
public class StartKafkaStreamsOff {

    public static final String START_KAFKA_STREAMS_OFF = "start-kafka-streams-off";
    public static final String STREAMS_OFF_INPUT = "streams-off-input";
    public static final String STREAMS_OFF_OUTPUT = "streams-off-output";

    @Singleton
    @Named(START_KAFKA_STREAMS_OFF)
    KStream<String, String> startKafkaStreamsOff(
        @Named(START_KAFKA_STREAMS_OFF) ConfiguredStreamBuilder builder) {

        // set default serdes
        Properties props = builder.getConfiguration();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        KStream<String, String> source = builder.stream(STREAMS_OFF_INPUT);
        source.to(STREAMS_OFF_OUTPUT);

        return source;
    }
}
