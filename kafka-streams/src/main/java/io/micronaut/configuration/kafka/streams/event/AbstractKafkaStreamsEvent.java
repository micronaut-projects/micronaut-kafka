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
