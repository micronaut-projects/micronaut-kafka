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
package io.micronaut.configuration.kafka.exceptions;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.messaging.exceptions.MessageListenerException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.util.Optional;

/**
 * Exception thrown when an error occurs processing a {@link ConsumerRecord} via a {@link io.micronaut.configuration.kafka.annotation.KafkaListener}.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@SuppressWarnings("WeakerAccess")
public class KafkaListenerException extends MessageListenerException {

    private final transient Object listener;
    private final transient Consumer<?, ?> kafkaConsumer;
    private final transient ConsumerRecords<?, ?> consumerRecords;
    private final transient ConsumerRecord<?, ?> consumerRecord;

    /**
     * Creates a new exception.
     *
     * @param message The message
     * @param listener The listener
     * @param kafkaConsumer The consumer
     * @param consumerRecord The consumer record
     */
    public KafkaListenerException(String message, Object listener, Consumer<?, ?> kafkaConsumer, ConsumerRecord<?, ?> consumerRecord) {
        this(message, null, listener, kafkaConsumer, consumerRecord);
    }

    /**
     * Creates a new exception.
     *
     * @param message The message
     * @param cause The cause
     * @param listener The listener
     * @param kafkaConsumer The consumer
     * @param consumerRecord The consumer record
     */
    public KafkaListenerException(String message, Throwable cause, Object listener, Consumer<?, ?> kafkaConsumer, ConsumerRecord<?, ?> consumerRecord) {
        this(message, cause, listener, kafkaConsumer, null, consumerRecord);
    }

    /**
     * Creates a new exception.
     *
     * @param cause The cause
     * @param listener The listener
     * @param kafkaConsumer The consumer
     * @param consumerRecord The consumer record
     */
    public KafkaListenerException(Throwable cause, Object listener, Consumer<?, ?> kafkaConsumer, ConsumerRecord<?, ?> consumerRecord) {
        this(cause.getMessage(), cause, listener, kafkaConsumer, consumerRecord);
    }

    /**
     * Creates a new exception.
     *
     * @param message The message
     * @param cause The cause
     * @param listener The listener
     * @param kafkaConsumer The consumer
     * @param consumerRecords The batch of consumer records
     * @param consumerRecord The consumer record
     */
    public KafkaListenerException(
        String message,
        Throwable cause,
        Object listener,
        Consumer<?, ?> kafkaConsumer,
        @Nullable ConsumerRecords<?, ?> consumerRecords,
        @Nullable ConsumerRecord<?, ?> consumerRecord
    ) {
        super(message, cause);
        this.listener = listener;
        this.kafkaConsumer = kafkaConsumer;
        this.consumerRecords = consumerRecords;
        this.consumerRecord = consumerRecord;
    }

    /**
     * @return The bean that is the kafka listener
     */
    public Object getKafkaListener() {
        return listener;
    }

    /**
     * @return The consumer that produced the error
     */
    @SuppressWarnings("java:S1452") // Remove usage of generic wildcard type
    public Consumer<?, ?> getKafkaConsumer() {
        return kafkaConsumer;
    }

    /**
     * @return The consumer record that was being processed that caused the error
     */
    @SuppressWarnings("java:S1452") // Remove usage of generic wildcard type
    public Optional<ConsumerRecord<?, ?>> getConsumerRecord() {
        return Optional.ofNullable(consumerRecord);
    }

    /**
     * @return The batch of consumer records that was being processed that caused the error
     * @since 5.3
     */
    @SuppressWarnings("java:S1452") // Remove usage of generic wildcard type
    public Optional<ConsumerRecords<?, ?>> getConsumerRecords() {
        return Optional.ofNullable(consumerRecords);
    }
}
