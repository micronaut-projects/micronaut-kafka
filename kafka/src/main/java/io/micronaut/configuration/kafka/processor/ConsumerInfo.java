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
import io.micronaut.configuration.kafka.seek.KafkaSeekOperations;
import io.micronaut.core.annotation.*;
import io.micronaut.core.reflect.ReflectionUtils;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.ArrayUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.messaging.Acknowledgement;
import io.micronaut.messaging.annotation.SendTo;
import io.micronaut.messaging.exceptions.MessagingSystemException;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Internal consumer info.
 */
@Internal
final class ConsumerInfo {
    final String clientId;
    @Nullable final String groupId;
    final boolean shouldRedeliver;
    final OffsetStrategy offsetStrategy;
    final ErrorStrategyValue errorStrategy;
    final @Nullable Duration retryDelay;
    final int retryCount;
    final boolean shouldHandleAllExceptions;
    final List<Class<? extends Throwable>> exceptionTypes;
    @Nullable final String producerClientId;
    @Nullable final String producerTransactionalId;
    final boolean isTransactional;
    final ExecutableMethod<Object, ?> method;
    final String logMethod;
    final boolean autoStartup;
    final boolean isBatch;
    final boolean isBlocking;
    final Duration pollTimeout;
    @Nullable final Argument<?> consumerArg;
    @Nullable final Argument<?> seekArg;
    @Nullable final Argument<?> ackArg;
    final boolean trackPartitions;
    final List<String> sendToTopics;
    final boolean shouldSendOffsetsToTransaction;
    final boolean returnsOneKafkaMessage;
    final boolean returnsManyKafkaMessages;

    @SuppressWarnings("unchecked")
    ConsumerInfo(
        String clientId,
        String groupId,
        OffsetStrategy offsetStrategy,
        AnnotationValue<KafkaListener> kafkaListener,
        ExecutableMethod<?, ?> method
    ) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.shouldRedeliver = kafkaListener.isTrue("redelivery");
        this.offsetStrategy = offsetStrategy;
        final Optional<AnnotationValue<ErrorStrategy>> errorStrategyAnnotation = kafkaListener.getAnnotation("errorStrategy", ErrorStrategy.class);
        this.errorStrategy = errorStrategyAnnotation.map(a -> a.getRequiredValue(ErrorStrategyValue.class)).orElse(ErrorStrategyValue.NONE); // NOSONAR
        this.retryDelay = errorStrategyAnnotation.flatMap(a -> a.get("retryDelay", Duration.class)).filter(d -> !d.isZero() && !d.isNegative()).orElse(null);
        this.retryCount = errorStrategyAnnotation.map(a -> a.intValue("retryCount").orElse(ErrorStrategy.DEFAULT_RETRY_COUNT)).orElse(0);
        this.shouldHandleAllExceptions = errorStrategyAnnotation.flatMap(a -> a.booleanValue("handleAllExceptions")).orElse(ErrorStrategy.DEFAULT_HANDLE_ALL_EXCEPTIONS);
        this.exceptionTypes = Arrays.stream((Class<? extends Throwable>[]) errorStrategyAnnotation.map(a -> a.classValues("exceptionTypes")).orElse(ReflectionUtils.EMPTY_CLASS_ARRAY)).toList();
        this.producerClientId = kafkaListener.stringValue("producerClientId").orElse(null);
        this.producerTransactionalId = kafkaListener.stringValue("producerTransactionalId").filter(StringUtils::isNotEmpty).orElse(null);
        this.isTransactional = producerTransactionalId != null;
        this.method = (ExecutableMethod<Object, ?>) method;
        this.logMethod = method.getDeclaringType().getSimpleName() + "#" + method.getName();
        this.autoStartup = kafkaListener.booleanValue("autoStartup").orElse(true);
        this.isBatch = method.isTrue(KafkaListener.class, "batch");
        this.isBlocking = method.hasAnnotation(Blocking.class);
        this.pollTimeout = method.getValue(KafkaListener.class, "pollTimeout", Duration.class).orElseGet(() -> Duration.ofMillis(100));
        this.consumerArg = Arrays.stream(method.getArguments()).filter(arg -> Consumer.class.isAssignableFrom(arg.getType())).findFirst().orElse(null);
        this.seekArg = Arrays.stream(method.getArguments()).filter(arg -> KafkaSeekOperations.class.isAssignableFrom(arg.getType())).findFirst().orElse(null);
        this.ackArg = Arrays.stream(method.getArguments()).filter(arg -> Acknowledgement.class.isAssignableFrom(arg.getType())).findFirst().orElse(null);
        this.trackPartitions = ackArg != null || offsetStrategy == OffsetStrategy.SYNC_PER_RECORD || offsetStrategy == OffsetStrategy.ASYNC_PER_RECORD;
        this.sendToTopics = Optional.ofNullable(method.stringValues(SendTo.class)).filter(ArrayUtils::isNotEmpty).stream().flatMap(Arrays::stream).toList();
        this.shouldSendOffsetsToTransaction = offsetStrategy == OffsetStrategy.SEND_TO_TRANSACTION;
        this.returnsOneKafkaMessage = method.getReturnType().getType().isAssignableFrom(KafkaMessage.class) || method.getReturnType().isAsyncOrReactive() && method.getReturnType().getFirstTypeVariable()
            .map(t -> t.getType().isAssignableFrom(KafkaMessage.class)).orElse(false);
        this.returnsManyKafkaMessages = Iterable.class.isAssignableFrom(method.getReturnType().getType()) && method.getReturnType().getFirstTypeVariable()
            .map(t -> t.getType().isAssignableFrom(KafkaMessage.class)).orElse(false);

        if (shouldSendOffsetsToTransaction) {
            if (!isTransactional || !method.hasAnnotation(SendTo.class)) {
                throw new MessagingSystemException("Offset strategy 'SEND_TO_TRANSACTION' can only be used when transaction is enabled and @SendTo is used");
            }
            if (shouldRedeliver) {
                throw new MessagingSystemException("Redelivery not supported for transactions in combination with @SendTo");
            }
        }
    }
}
