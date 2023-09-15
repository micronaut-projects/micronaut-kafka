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
package io.micronaut.configuration.kafka.processor;

import io.micronaut.configuration.kafka.KafkaMessage;
import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.reflect.ReflectionUtils;
import io.micronaut.core.type.ReturnType;
import io.micronaut.core.util.ArrayUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.messaging.annotation.SendTo;
import io.micronaut.messaging.exceptions.MessagingSystemException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * The internal state of the consumer.
 *
 * @author Denis Stepanov
 */
final class ConsumerState {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerProcessor.class);

    final String clientId;
    final Consumer<?, ?> kafkaConsumer;
    final Object consumerBean;
    final Set<String> subscriptions;

    Set<TopicPartition> assignments;
    Set<TopicPartition> _pausedTopicPartitions;
    Set<TopicPartition> _pauseRequests;

    @Nullable
    final String groupId;
    final boolean redelivery;

    final OffsetStrategy offsetStrategy;

    final ErrorStrategyValue errorStrategy;
    @Nullable
    final Duration errorStrategyRetryDelay;
    final int errorStrategyRetryCount;
    final boolean handleAllExceptions;
    final Class<? extends Throwable>[] errorStrategyExceptions;

    @Nullable
    Map<Integer, PartitionRetryState> partitionRetries;

    boolean autoPaused;
    final String producerClientId;
    final String producerTransactionalId;
    final boolean transactional;

    @Nullable
    final String[] sendToDestinationTopics;
    final boolean useSendOffsetsToTransaction;
    final boolean isMessageReturnType;
    final boolean isMessagesIterableReturnType;
    volatile ConsumerCloseState closedState;

    ConsumerState(String clientId, String groupId, OffsetStrategy offsetStrategy, Consumer<?, ?> consumer, Object consumerBean, Set<String> subscriptions,
        AnnotationValue<KafkaListener> kafkaListener, ExecutableMethod<?, ?> method) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.kafkaConsumer = consumer;
        this.consumerBean = consumerBean;
        this.subscriptions = subscriptions;
        this.offsetStrategy = offsetStrategy;

        redelivery = kafkaListener.isTrue("redelivery");

        AnnotationValue<ErrorStrategy> errorStrategyAnnotation = kafkaListener.getAnnotation("errorStrategy", ErrorStrategy.class).orElse(null);
        errorStrategy = errorStrategyAnnotation != null
            ? errorStrategyAnnotation.getRequiredValue(ErrorStrategyValue.class)
            : ErrorStrategyValue.NONE;

        if (errorStrategy.isRetry()) {
            Duration retryDelay = errorStrategyAnnotation.get("retryDelay", Duration.class)
                .orElse(Duration.ofSeconds(ErrorStrategy.DEFAULT_DELAY_IN_SECONDS));
            this.errorStrategyRetryDelay = retryDelay.isNegative() || retryDelay.isZero() ? null : retryDelay;
            this.errorStrategyRetryCount = errorStrategyAnnotation.intValue("retryCount").orElse(ErrorStrategy.DEFAULT_RETRY_COUNT);
            this.handleAllExceptions = errorStrategyAnnotation.booleanValue("handleAllExceptions").orElse(ErrorStrategy.DEFAULT_HANDLE_ALL_EXCEPTIONS);
            //noinspection unchecked
            this.errorStrategyExceptions = (Class<? extends Throwable>[]) errorStrategyAnnotation.classValues("exceptionTypes");
        } else {
            this.errorStrategyRetryDelay = null;
            this.errorStrategyRetryCount = 0;
            this.handleAllExceptions = false;
            //noinspection unchecked
            this.errorStrategyExceptions = (Class<? extends Throwable>[]) ReflectionUtils.EMPTY_CLASS_ARRAY;
        }

        autoPaused = !kafkaListener.booleanValue("autoStartup").orElse(true);
        producerClientId = kafkaListener.stringValue("producerClientId").orElse(null);
        producerTransactionalId = kafkaListener.stringValue("producerTransactionalId").orElse(null);
        transactional = StringUtils.isNotEmpty(producerTransactionalId);

        String[] destinationTopics = method.stringValues(SendTo.class);
        sendToDestinationTopics = ArrayUtils.isNotEmpty(destinationTopics) ? destinationTopics : null;

        if (offsetStrategy == OffsetStrategy.SEND_TO_TRANSACTION) {
            if (transactional && method.hasAnnotation(SendTo.class)) {
                useSendOffsetsToTransaction = true;
            } else {
                throw new MessagingSystemException("Offset strategy 'SEND_TO_TRANSACTION' can only be used when transaction is enabled and @SendTo is used");
            }
        } else {
            useSendOffsetsToTransaction = false;
        }

        if (useSendOffsetsToTransaction && redelivery) {
            throw new MessagingSystemException("Redelivery not supported for transactions in combination with @SendTo");
        }
        ReturnType<?> returnType = method.getReturnType();
        isMessageReturnType = returnType.getType().isAssignableFrom(KafkaMessage.class)
            || returnType.isAsyncOrReactive() && returnType.getFirstTypeVariable().map(t -> t.getType().isAssignableFrom(KafkaMessage.class)).orElse(false);
        isMessagesIterableReturnType = Iterable.class.isAssignableFrom(returnType.getType()) && returnType.getFirstTypeVariable().map(t -> t.getType().isAssignableFrom(KafkaMessage.class)).orElse(false);
        closedState = ConsumerCloseState.NOT_STARTED;
    }

    void pause() {
        pause(assignments);
    }

    synchronized void pause(@NonNull Collection<TopicPartition> topicPartitions) {
        if (_pauseRequests == null) {
            _pauseRequests = new HashSet<>();
        }
        _pauseRequests.addAll(topicPartitions);
    }

    synchronized void resume() {
        autoPaused = false;
        _pauseRequests = null;
    }

    synchronized void resume(@NonNull Collection<TopicPartition> topicPartitions) {
        autoPaused = false;
        if (_pauseRequests != null) {
            _pauseRequests.removeAll(topicPartitions);
        }
    }

    synchronized boolean isPaused(@NonNull Collection<TopicPartition> topicPartitions) {
        if (_pauseRequests == null || _pausedTopicPartitions == null) {
            return false;
        }
        return _pauseRequests.containsAll(topicPartitions) && _pausedTopicPartitions.containsAll(topicPartitions);
    }

    synchronized void resumeTopicPartitions() {
        Set<TopicPartition> paused = kafkaConsumer.paused();
        if (paused.isEmpty()) {
            return;
        }
        final List<TopicPartition> toResume = paused.stream()
            .filter(topicPartition -> _pauseRequests == null || !_pauseRequests.contains(topicPartition))
            .toList();
        if (!toResume.isEmpty()) {
            LOG.debug("Resuming Kafka consumption for Consumer [{}] from topic partition: {}", clientId, toResume);
            kafkaConsumer.resume(toResume);
        }
        if (_pausedTopicPartitions != null) {
            toResume.forEach(_pausedTopicPartitions::remove);
        }
    }

    synchronized void pauseTopicPartitions() {
        if (_pauseRequests == null || _pauseRequests.isEmpty()) {
            return;
        }
        // Only attempt to pause active assignments
        Set<TopicPartition> validPauseRequests = new HashSet<>(_pauseRequests);
        validPauseRequests.retainAll(assignments);
        if (validPauseRequests.isEmpty()) {
            return;
        }
        LOG.trace("Pausing Kafka consumption for Consumer [{}] from topic partition: {}", clientId, validPauseRequests);
        kafkaConsumer.pause(validPauseRequests);
        LOG.debug("Paused Kafka consumption for Consumer [{}] from topic partition: {}", clientId, kafkaConsumer.paused());
        if (_pausedTopicPartitions == null) {
            _pausedTopicPartitions = new HashSet<>();
        }
        _pausedTopicPartitions.addAll(validPauseRequests);
    }
}
