/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.streams.event;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Event fired after the Kafka streams have started.
 *
 * @author graemerocher
 * @since 2.0.0
 */
public class AfterKafkaStreamsStart extends AbstractKafkaStreamsEvent {
    private final KStream<?, ?>[] streams;

    /**
     * Default constructor.
     * @param kafkaStreams The kafka streams object
     * @param streams The kstreams
     */
    public AfterKafkaStreamsStart(KafkaStreams kafkaStreams, KStream<?, ?>[] streams) {
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
