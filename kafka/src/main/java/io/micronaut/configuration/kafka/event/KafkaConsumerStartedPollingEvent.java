/*
 * Copyright 2017-2023 original authors
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
package io.micronaut.configuration.kafka.event;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * An event fired after a Kafka {@link Consumer} executes the first polling.
 *
 * @author Jorge F. Sanchez
 * @since 4.5.3
 */
public final class KafkaConsumerStartedPollingEvent extends AbstractKafkaApplicationEvent<Consumer> {
    /**
     * Constructs an event with a given Consumer source.
     *
     * @param consumer The Consumer on which the Event initially occurred.
     * @throws IllegalArgumentException if source is null.
     */
    public KafkaConsumerStartedPollingEvent(Consumer consumer) {
        super(consumer);
    }
}
