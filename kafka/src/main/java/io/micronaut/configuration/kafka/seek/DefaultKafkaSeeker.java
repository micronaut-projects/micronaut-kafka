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
package io.micronaut.configuration.kafka.seek;

import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * Default implementation of {@link KafkaSeeker}.
 *
 * @param consumer the consumer on which to perform the {@link KafkaSeekOperations seek operations}.
 * @author Guillermo Calvo
 * @see KafkaSeeker
 * @since 4.1
 */
@Internal
record DefaultKafkaSeeker(@NonNull Consumer<?, ?> consumer) implements KafkaSeeker {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultKafkaSeeker.class);

    /**
     * Creates a new instance.
     *
     * @param consumer the consumer on which to perform the {@link KafkaSeekOperations seek operations}.
     */
    public DefaultKafkaSeeker {
        Objects.requireNonNull(consumer, "consumer");
    }

    @Override
    public boolean perform(@NonNull KafkaSeekOperation operation) {
        try {
            final TopicPartition tp = operation.topicPartition();
            if (operation.offset() == 0) {
                switch (operation.offsetType()) {
                    case FORWARD, BACKWARD:
                        // Special case: relative zero-offset
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Relative zero-offset seek operation dropped: {}", operation);
                        }
                        return false;
                    case BEGINNING:
                        // Optimized case: seek to the beginning
                        consumer.seekToBeginning(singletonList(tp));
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Seek to the beginning operation succeeded: {}-{}", operation.topic(), operation.partition());
                        }
                        return true;
                    case END:
                        // Optimized case: seek to the end
                        consumer.seekToEnd(singletonList(tp));
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Seek to the end operation succeeded: {}-{}", operation.topic(), operation.partition());
                        }
                        return true;
                    default:
                        // Perform operation regularly
                }
            }
            final long offset = switch (operation.offsetType()) {
                case ABSOLUTE -> operation.offset();
                case FORWARD -> current(tp) + operation.offset();
                case BACKWARD -> current(tp) - operation.offset();
                case BEGINNING -> beginning(tp) + operation.offset();
                case END -> end(tp) - operation.offset();
                case TIMESTAMP -> earliest(tp, operation.offset()).orElseGet(() -> end(tp));
            };
            consumer.seek(tp, Math.max(0, offset));
            if (LOG.isInfoEnabled()) {
                LOG.info("Seek operation succeeded: {} - offset: {}", operation, offset);
            }
            return true;
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Seek operation failed: {}", operation, e);
            }
            return false;
        }
    }

    private long current(TopicPartition tp) {
        return consumer.position(tp);
    }

    public long beginning(TopicPartition tp) {
        return consumer.beginningOffsets(singletonList(tp)).get(tp);
    }

    private long end(TopicPartition tp) {
        return consumer.endOffsets(singletonList(tp)).get(tp);
    }

    private Optional<Long> earliest(TopicPartition tp, long ts) {
        return Optional.ofNullable(consumer.offsetsForTimes(singletonMap(tp, ts)).get(tp))
            .map(OffsetAndTimestamp::offset);
    }
}
