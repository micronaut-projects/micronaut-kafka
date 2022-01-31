package io.micronaut.configuration.kafka.streams.optimization;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

@Factory
public class OptimizationStream {

    public static final String STREAM_OPTIMIZATION_ON = "optimization-on";
    public static final String OPTIMIZATION_ON_INPUT = "optimization-on-input";
    public static final String OPTIMIZATION_ON_STORE = "on-store";

    @Singleton
    @Named(STREAM_OPTIMIZATION_ON)
    KStream<String, String> optimizationOn(
            @Named(STREAM_OPTIMIZATION_ON) ConfiguredStreamBuilder builder) {
        // set default serdes
        Properties props = builder.getConfiguration();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KTable<String, String> table = builder.table(
                OPTIMIZATION_ON_INPUT,
                Materialized.as(OPTIMIZATION_ON_STORE)
        );

        return table.toStream();
    }

    public static final String STREAM_OPTIMIZATION_OFF = "optimization-off";
    public static final String OPTIMIZATION_OFF_INPUT = "optimization-off-input";
    public static final String OPTIMIZATION_OFF_STORE = "off-store";

    @Singleton
    @Named(STREAM_OPTIMIZATION_OFF)
    KStream<String, String> optimizationOff(
            @Named(STREAM_OPTIMIZATION_OFF) ConfiguredStreamBuilder builder) {
        // set default serdes
        Properties props = builder.getConfiguration();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KTable<String, String> table = builder.table(
                OPTIMIZATION_OFF_INPUT,
                Materialized.as(OPTIMIZATION_OFF_STORE)
        );

        return table.toStream();
    }
}
