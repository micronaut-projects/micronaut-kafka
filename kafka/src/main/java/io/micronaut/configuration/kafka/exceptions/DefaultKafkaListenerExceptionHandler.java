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

import io.micronaut.configuration.kafka.config.DefaultKafkaListenerExceptionHandlerConfiguration;
import io.micronaut.context.annotation.Primary;
import io.micronaut.core.annotation.NonNull;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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

    private boolean skipRecordOnDeserializationFailure;
    private boolean commitRecordOnDeserializationFailure;

    /**
     * Creates a new instance.
     *
     * @param config The default Kafka listener exception handler configuration
     */
    public DefaultKafkaListenerExceptionHandler(DefaultKafkaListenerExceptionHandlerConfiguration config) {
        skipRecordOnDeserializationFailure = config.isSkipRecordOnDeserializationFailure();
        commitRecordOnDeserializationFailure = config.isCommitRecordOnDeserializationFailure();
    }

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
     * Sets whether to commit the offset of past records that are not deserializable and are skipped.
     * @param commitRecordOnDeserializationFailure True if the offset for records that are not deserializable should be committed after being skipped.
     */
    public void setCommitRecordOnDeserializationFailure(boolean commitRecordOnDeserializationFailure) {
        this.commitRecordOnDeserializationFailure = commitRecordOnDeserializationFailure;
    }

    /**
     * Seeks past a serialization exception if an error occurs. Additionally commits the offset if commitRecordOnDeserializationFailure is set
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
            LOG.debug("Extracting unserializable consumer record topic, partition and offset from error message: {}", message);
            final Matcher matcher = SERIALIZATION_EXCEPTION_MESSAGE_PATTERN.matcher(message);
            if (matcher.find()) {
                final String topic = matcher.group(1);
                final int partition = Integer.valueOf(matcher.group(2));
                final int offset = Integer.valueOf(matcher.group(3));
                TopicPartition tp = new TopicPartition(topic, partition);
                LOG.debug("Seeking past unserializable consumer record for partition {}-{} and offset {}", topic, partition, offset);

                try {
                    kafkaConsumer.seek(tp, offset + 1);
                } catch (Throwable e) {
                    LOG.error("Kafka consumer [{}] failed to seek past unserializable value: {}", consumerBean, e.getMessage(), e);
                }

                if (this.commitRecordOnDeserializationFailure) {
                    try {
                        LOG.debug("Permanently skipping unserializable consumer record by committing offset {} for partition {}-{}", offset, topic, partition);
                        kafkaConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(offset + 1)));
                    } catch (Throwable e) {
                        LOG.error("Kafka consumer [{}] failed to commit offset of unserializable value: {}", consumerBean, e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable e) {
            LOG.error("Failed to extract topic, partition and offset from serialization error message: {}", cause.getMessage(), e);
        }
    }
}
