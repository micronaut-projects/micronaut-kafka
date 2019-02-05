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
package io.micronaut.configuration.kafka.streams;

// tag::imports[]
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.*;
// end::imports[]

// tag::clazz[]
@Factory
public class WordCountStream {

    public static final String INPUT = "streams-plaintext-input"; // <1>
    public static final String OUTPUT = "streams-wordcount-output"; // <2>

// end::clazz[]

    // tag::wordCountStream[]
    @Singleton
    KStream<String, String> wordCountStream(ConfiguredStreamBuilder builder) { // <3>
        // set default serdes
        Properties props = builder.getConfiguration();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStream<String, String> source = builder.stream(INPUT);
        KTable<String, Long> counts = source
                .flatMapValues( value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
                .count();

        // need to override value serde to Long type
        counts.toStream().to(OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));
        return source;
    }
    // end::wordCountStream[]

    // tag::namedStream[]
    @Singleton
    KStream<String, String> myStream(
            @Named("my-stream") ConfiguredStreamBuilder builder) {

    // end::namedStream[]
        // set default serdes
        Properties props = builder.getConfiguration();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStream<String, String> source = builder.stream("named-word-count-input");
        KTable<String, Long> counts = source
                .flatMapValues( value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
                .count();

        // need to override value serde to Long type
        counts.toStream().to("named-word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        return source;
    }
}
