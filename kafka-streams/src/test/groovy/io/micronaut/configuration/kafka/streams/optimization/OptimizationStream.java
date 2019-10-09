/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.streams.optimization;


import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import javax.inject.Named;
import javax.inject.Singleton;
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

        KTable<String, String> table = builder
                .table(OPTIMIZATION_ON_INPUT, Materialized.as(OPTIMIZATION_ON_STORE));

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

        KTable<String, String> table = builder
                .table(OPTIMIZATION_OFF_INPUT, Materialized.as(OPTIMIZATION_OFF_STORE));

        return table.toStream();
    }
}
