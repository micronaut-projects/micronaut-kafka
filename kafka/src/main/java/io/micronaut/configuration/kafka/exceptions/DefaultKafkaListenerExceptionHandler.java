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

import io.micronaut.context.annotation.Primary;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micronaut.core.annotation.NonNull;
import javax.inject.Singleton;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The default ExceptionHandler used when a {@link org.apache.kafka.clients.consumer.KafkaConsumer}
 * fails to process a {@link org.apache.kafka.clients.consumer.ConsumerRecord}. By default just logs the error.
 *
 * @author graemerocher
 * @since 1.0
 */
@Singleton
@Primary
public class DefaultKafkaListenerExceptionHandler implements KafkaListenerExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaListenerExceptionHandler.class);
    private static final Pattern SERIALIZATION_EXCEPTION_MESSAGE_PATTERN = Pattern.compile(".+ for partition (.+)-(\\d+) at offset (\\d+)\\..+");

    private boolean skipRecordOnDeserializationFailure = true;

    @Override
    public void handle(KafkaListenerException exception) {
        final Throwable cause = exception.getCause();
        final Object consumerBean = exception.getKafkaListener();
        if (cause instanceof SerializationException) {
            LOG.error("Kafka consumer [{}] failed to deserialize value: {}", consumerBean, cause.getMessage(), cause);

            if (skipRecordOnDeserializationFailure) {
                final Consumer<?, ?> kafkaConsumer = exception.getKafkaConsumer();
                seekPastDeserializationError((SerializationException) cause, consumerBean, kafkaConsumer);
            }
        } else {
            if (LOG.isErrorEnabled()) {
                Optional<ConsumerRecord<?, ?>> consumerRecord = exception.getConsumerRecord();
                if (consumerRecord.isPresent()) {
                    LOG.error("Error processing record [{}] for Kafka consumer [{}] produced error: {}", consumerRecord, consumerBean, cause.getMessage(), cause);
                } else {
                    LOG.error("Kafka consumer [{}] produced error: {}", consumerBean, cause.getMessage(), cause);
                }
            }
        }
    }

    /**
     * Sets whether the seek past records that are not deserializable.
     * @param skipRecordOnDeserializationFailure True if records that are not deserializable should be skipped.
     */
    public void setSkipRecordOnDeserializationFailure(boolean skipRecordOnDeserializationFailure) {
        this.skipRecordOnDeserializationFailure = skipRecordOnDeserializationFailure;
    }

    /**
     * Seeks past a serialization exception if an error occurs.
     * @param cause The cause
     * @param consumerBean The consumer bean
     * @param kafkaConsumer The kafka consumer
     */
    protected void seekPastDeserializationError(
            @NonNull SerializationException cause,
            @NonNull Object consumerBean,
            @NonNull Consumer<?, ?> kafkaConsumer) {
        try {
            final String message = cause.getMessage();
            final Matcher matcher = SERIALIZATION_EXCEPTION_MESSAGE_PATTERN.matcher(message);
            if (matcher.find()) {
                final String topic = matcher.group(1);
                final int partition = Integer.valueOf(matcher.group(2));
                final int offset = Integer.valueOf(matcher.group(3));
                TopicPartition tp = new TopicPartition(topic, partition);
                LOG.debug("Seeking past unserializable consumer record for partition {}-{} and offset {}", topic, partition, offset);
                kafkaConsumer.seek(tp, offset + 1);
            }
        } catch (Throwable e) {
            LOG.error("Kafka consumer [{}] failed to seek past unserializable value: {}", consumerBean, e.getMessage(), e);
        }
    }
}
