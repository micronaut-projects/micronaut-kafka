/*
 * Copyright 2017-2026 original authors
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

import io.micronaut.configuration.kafka.ConsumerRecordInterceptor;
import io.micronaut.configuration.kafka.KafkaMessage;
import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.exceptions.OffsetCommitExceptionLogger;
import io.micronaut.configuration.kafka.seek.KafkaSeekOperations;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Blocking;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.reflect.ReflectionUtils;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.ArrayUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.messaging.Acknowledgement;
import io.micronaut.messaging.annotation.SendTo;
import io.micronaut.messaging.exceptions.MessagingSystemException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Internal consumer info.
 *
 * @author Guillermo Calvo
 * @since 5.2
 */
@Internal
final class ConsumerInfo {
    final String clientId;
    @Nullable final String groupId;
    final boolean shouldRedeliver;
    final OffsetStrategy offsetStrategy;
    final ErrorStrategyValue errorStrategy;
    @Nullable final String dlq;
    @Nullable final Duration retryDelay;
    final int retryCount;
    final boolean shouldHandleAllExceptions;
    final boolean shouldStopOnExhaustedRetry;
    final List<Class<? extends Throwable>> exceptionTypes;
    @Nullable final String producerClientId;
    @Nullable final String producerTransactionalId;
    final boolean isTransactional;
    final ExecutableMethod<Object, ?> method;
    final boolean autoStartup;
    final boolean isBatch;
    final Duration pollTimeout;
    final boolean trackPartitions;
    final boolean shouldSendOffsetsToTransaction;
    final boolean cooperativeStickyAssignmentStrategy;
    @Nullable final NonBlockingRetryTopics nonBlockingRetryTopics;
    private final List<ExecutableMethod<Object, ?>> listenerMethods;
    private final Map<String, ExecutableMethod<Object, ?>> topicMethods = new HashMap<>();
    private final List<PatternMethod> patternMethods;
    private final Map<String, ExecutableMethod<Object, ?>> resolvedMethods = new ConcurrentHashMap<>();
    private final Map<ExecutableMethod<Object, ?>, Optional<Argument<?>>> consumerArgCache = new ConcurrentHashMap<>();
    private final Map<ExecutableMethod<Object, ?>, Optional<Argument<?>>> seekArgCache = new ConcurrentHashMap<>();
    private final Map<ExecutableMethod<Object, ?>, Optional<Argument<?>>> ackArgCache = new ConcurrentHashMap<>();
    private final Map<ExecutableMethod<Object, ?>, List<String>> sendToTopicsCache = new ConcurrentHashMap<>();
    private final Map<ExecutableMethod<Object, ?>, Boolean> returnsOneKafkaMessageCache = new ConcurrentHashMap<>();
    private final Map<ExecutableMethod<Object, ?>, Boolean> returnsManyKafkaMessagesCache = new ConcurrentHashMap<>();
    private final Map<ExecutableMethod<Object, ?>, List<ConsumerRecordInterceptor<?, ?>>> consumerRecordInterceptorsByMethod;

    ConsumerInfo(
        String clientId,
        String groupId,
        OffsetStrategy offsetStrategy,
        AnnotationValue<KafkaListener> kafkaListener,
        Properties properties,
        ExecutableMethod<?, ?> method
    ) {
        this(clientId, groupId, offsetStrategy, kafkaListener, properties, List.of(method), Map.of());
    }

    ConsumerInfo(
        String clientId,
        String groupId,
        OffsetStrategy offsetStrategy,
        AnnotationValue<KafkaListener> kafkaListener,
        Properties properties,
        ExecutableMethod<?, ?> method,
        Collection<ConsumerRecordInterceptor<?, ?>> consumerRecordInterceptors
    ) {
        this(
            clientId,
            groupId,
            offsetStrategy,
            kafkaListener,
            properties,
            List.of(method),
            Map.of(method, List.copyOf(consumerRecordInterceptors))
        );
    }

    ConsumerInfo(
        String clientId,
        String groupId,
        OffsetStrategy offsetStrategy,
        AnnotationValue<KafkaListener> kafkaListener,
        Properties properties,
        ExecutableMethod<?, ?> method,
        List<AnnotationValue<Topic>> topicAnnotations
    ) {
        this(clientId, groupId, offsetStrategy, kafkaListener, properties, List.of(method), topicAnnotations, Map.of());
    }

    ConsumerInfo(
        String clientId,
        String groupId,
        OffsetStrategy offsetStrategy,
        AnnotationValue<KafkaListener> kafkaListener,
        Properties properties,
        List<ExecutableMethod<?, ?>> methods
    ) {
        this(clientId, groupId, offsetStrategy, kafkaListener, properties, methods, methods.stream()
            .flatMap(executableMethod -> executableMethod.getDeclaredAnnotationValuesByType(Topic.class).stream())
            .toList(), Map.of());
    }

    @SuppressWarnings("unchecked")
    ConsumerInfo(
        String clientId,
        String groupId,
        OffsetStrategy offsetStrategy,
        AnnotationValue<KafkaListener> kafkaListener,
        Properties properties,
        List<ExecutableMethod<?, ?>> methods,
        Map<ExecutableMethod<?, ?>, List<ConsumerRecordInterceptor<?, ?>>> consumerRecordInterceptorsByMethod
    ) {
        this(clientId, groupId, offsetStrategy, kafkaListener, properties, methods, methods.stream()
            .flatMap(executableMethod -> executableMethod.getDeclaredAnnotationValuesByType(Topic.class).stream())
            .toList(), consumerRecordInterceptorsByMethod);
    }

    ConsumerInfo(
        String clientId,
        String groupId,
        OffsetStrategy offsetStrategy,
        AnnotationValue<KafkaListener> kafkaListener,
        Properties properties,
        List<ExecutableMethod<?, ?>> methods,
        List<AnnotationValue<Topic>> topicAnnotations,
        Map<ExecutableMethod<?, ?>, List<ConsumerRecordInterceptor<?, ?>>> consumerRecordInterceptorsByMethod
    ) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.shouldRedeliver = kafkaListener.isTrue("redelivery");
        this.offsetStrategy = offsetStrategy;
        final Optional<AnnotationValue<ErrorStrategy>> errorStrategyAnnotation = kafkaListener.getAnnotation("errorStrategy", ErrorStrategy.class);
        this.errorStrategy = errorStrategyAnnotation.map(a -> a.getRequiredValue(ErrorStrategyValue.class)).orElse(ErrorStrategyValue.NONE); // NOSONAR
        if (this.errorStrategy.isRetryTopic() && offsetStrategy == OffsetStrategy.SEND_TO_TRANSACTION) {
            throw new MessagingSystemException("Error strategy 'RETRY_TOPIC_ON_ERROR' cannot be used with offset strategy 'SEND_TO_TRANSACTION'");
        }
        this.dlq = errorStrategyAnnotation.flatMap(a -> a.stringValue("dlq")).filter(StringUtils::isNotEmpty).orElse(null);
        if (this.errorStrategy == ErrorStrategyValue.LOG_AND_RESUME_AT_NEXT_RECORD && this.dlq == null) {
            throw new MessagingSystemException("Error strategy 'LOG_AND_RESUME_AT_NEXT_RECORD' requires setting a non-empty dead letter topic with 'dlq'");
        }
        this.retryDelay = errorStrategyAnnotation.flatMap(a -> a.get("retryDelay", Duration.class)).filter(d -> !d.isZero() && !d.isNegative()).orElse(null);
        this.retryCount = errorStrategyAnnotation.map(a -> a.intValue("retryCount").orElse(ErrorStrategy.DEFAULT_RETRY_COUNT)).orElse(0);
        this.shouldHandleAllExceptions = errorStrategyAnnotation.flatMap(a -> a.booleanValue("handleAllExceptions")).orElse(ErrorStrategy.DEFAULT_HANDLE_ALL_EXCEPTIONS);
        this.shouldStopOnExhaustedRetry = errorStrategyAnnotation.flatMap(a -> a.booleanValue("stopOnExhaustedRetry")).orElse(false);
        if (this.shouldStopOnExhaustedRetry && !this.errorStrategy.isRetry()) {
            throw new MessagingSystemException("'stopOnExhaustedRetry' requires a retry error strategy");
        }
        this.exceptionTypes = Arrays.stream((Class<? extends Throwable>[]) errorStrategyAnnotation.map(a -> a.classValues("exceptionTypes")).orElse(ReflectionUtils.EMPTY_CLASS_ARRAY)).toList();
        this.producerClientId = kafkaListener.stringValue("producerClientId").orElse(null);
        this.producerTransactionalId = kafkaListener.stringValue("producerTransactionalId").filter(StringUtils::isNotEmpty).orElse(null);
        this.isTransactional = producerTransactionalId != null;
        this.cooperativeStickyAssignmentStrategy = OffsetCommitExceptionLogger.isCooperativeStickyAssignor(properties.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));
        java.util.ArrayList<ExecutableMethod<Object, ?>> resolvedListenerMethods = new java.util.ArrayList<>(methods.size());
        Map<ExecutableMethod<Object, ?>, List<ConsumerRecordInterceptor<?, ?>>> resolvedInterceptors = new HashMap<>(methods.size());
        for (ExecutableMethod<?, ?> executableMethod : methods) {
            ExecutableMethod<Object, ?> resolvedMethod = (ExecutableMethod<Object, ?>) executableMethod;
            resolvedListenerMethods.add(resolvedMethod);
            resolvedInterceptors.put(
                resolvedMethod,
                List.copyOf(consumerRecordInterceptorsByMethod.getOrDefault(executableMethod, List.of()))
            );
        }
        this.listenerMethods = List.copyOf(resolvedListenerMethods);
        this.consumerRecordInterceptorsByMethod = Map.copyOf(resolvedInterceptors);
        this.method = this.listenerMethods.get(0);
        this.isBatch = method.isTrue(KafkaListener.class, "batch");
        this.nonBlockingRetryTopics = NonBlockingRetryTopics.create(kafkaListener, topicAnnotations, this.isBatch);
        this.patternMethods = resolveTopicMethods(this.listenerMethods);
        this.autoStartup = kafkaListener.booleanValue("autoStartup").orElse(true);
        this.pollTimeout = this.method.getValue(KafkaListener.class, "pollTimeout", Duration.class).orElseGet(() -> Duration.ofMillis(100));
        this.trackPartitions = anyMethodHasAckArg() || offsetStrategy == OffsetStrategy.SYNC_PER_RECORD || offsetStrategy == OffsetStrategy.ASYNC_PER_RECORD;
        this.shouldSendOffsetsToTransaction = offsetStrategy == OffsetStrategy.SEND_TO_TRANSACTION;

        if (shouldSendOffsetsToTransaction) {
            if (!this.listenerMethods.stream().allMatch(executableMethod -> executableMethod.hasAnnotation(SendTo.class)) || !isTransactional) {
                throw new MessagingSystemException("Offset strategy 'SEND_TO_TRANSACTION' can only be used when transaction is enabled and @SendTo is used");
            }
            if (shouldRedeliver) {
                throw new MessagingSystemException("Redelivery not supported for transactions in combination with @SendTo");
            }
        }
    }

    boolean routesByTopic() {
        return listenerMethods.size() > 1;
    }

    @SuppressWarnings("java:S1452")
    ExecutableMethod<Object, ?> methodForTopic(String topic) {
        if (topicMethods.isEmpty() && patternMethods.isEmpty()) {
            return method;
        }
        return resolvedMethods.computeIfAbsent(topic, this::resolveMethod);
    }

    String logMethod(String topic) {
        ExecutableMethod<Object, ?> executableMethod = methodForTopic(topic);
        return executableMethod.getDeclaringType().getSimpleName() + "#" + executableMethod.getName();
    }

    boolean isBlocking(String topic) {
        return methodForTopic(topic).hasAnnotation(Blocking.class);
    }

    List<ConsumerRecordInterceptor<?, ?>> consumerRecordInterceptors(String topic) {
        return consumerRecordInterceptorsByMethod.getOrDefault(methodForTopic(topic), List.of());
    }

    List<String> sendToTopics(String topic) {
        return sendToTopicsCache.computeIfAbsent(methodForTopic(topic), executableMethod ->
            Optional.ofNullable(executableMethod.stringValues(SendTo.class))
                .filter(ArrayUtils::isNotEmpty)
                .stream()
                .flatMap(Arrays::stream)
                .toList()
        );
    }

    boolean returnsOneKafkaMessage(String topic) {
        return returnsOneKafkaMessageCache.computeIfAbsent(methodForTopic(topic), executableMethod -> {
            var returnType = executableMethod.getReturnType();
            return returnType.getType().isAssignableFrom(KafkaMessage.class) ||
                (returnType.isAsyncOrReactive() && returnType.getFirstTypeVariable()
                    .map(t -> t.getType().isAssignableFrom(KafkaMessage.class))
                    .orElse(false));
        });
    }

    boolean returnsManyKafkaMessages(String topic) {
        return returnsManyKafkaMessagesCache.computeIfAbsent(methodForTopic(topic), executableMethod -> {
            var returnType = executableMethod.getReturnType();
            return Iterable.class.isAssignableFrom(returnType.getType()) &&
                returnType.getFirstTypeVariable().map(t -> t.getType().isAssignableFrom(KafkaMessage.class)).orElse(false);
        });
    }

    @Nullable
    @SuppressWarnings("java:S1452")
    Argument<?> consumerArg(String topic) {
        return consumerArgCache.computeIfAbsent(methodForTopic(topic), executableMethod ->
            Arrays.stream(executableMethod.getArguments())
                .filter(arg -> Consumer.class.isAssignableFrom(arg.getType()))
                .findFirst()
        ).orElse(null);
    }

    @Nullable
    @SuppressWarnings("java:S1452")
    Argument<?> seekArg(String topic) {
        return seekArgCache.computeIfAbsent(methodForTopic(topic), executableMethod ->
            Arrays.stream(executableMethod.getArguments())
                .filter(arg -> KafkaSeekOperations.class.isAssignableFrom(arg.getType()))
                .findFirst()
        ).orElse(null);
    }

    @Nullable
    @SuppressWarnings("java:S1452")
    Argument<?> ackArg(String topic) {
        return ackArgCache.computeIfAbsent(methodForTopic(topic), executableMethod ->
            Arrays.stream(executableMethod.getArguments())
                .filter(arg -> Acknowledgement.class.isAssignableFrom(arg.getType()))
                .findFirst()
        ).orElse(null);
    }

    private boolean anyMethodHasAckArg() {
        return listenerMethods.stream()
            .anyMatch(executableMethod -> Arrays.stream(executableMethod.getArguments()).anyMatch(arg -> Acknowledgement.class.isAssignableFrom(arg.getType())));
    }

    private List<PatternMethod> resolveTopicMethods(List<ExecutableMethod<Object, ?>> methods) {
        List<PatternMethod> patterns = new java.util.ArrayList<>();
        for (ExecutableMethod<Object, ?> executableMethod : methods) {
            registerTopicMethods(patterns, executableMethod);
        }
        return List.copyOf(patterns);
    }

    private static List<AnnotationValue<Topic>> topicAnnotations(ExecutableMethod<Object, ?> executableMethod) {
        return executableMethod.getDeclaredAnnotationValuesByType(Topic.class);
    }

    private ExecutableMethod<Object, ?> resolveMethod(String topic) {
        ExecutableMethod<Object, ?> executableMethod = topicMethods.get(topic);
        if (executableMethod != null) {
            return executableMethod;
        }
        ExecutableMethod<Object, ?> matchedMethod = null;
        for (PatternMethod patternMethod : patternMethods) {
            if (patternMethod.pattern.matcher(topic).matches()) {
                if (matchedMethod != null && matchedMethod != patternMethod.method) {
                    throw new MessagingSystemException("Multiple @Topic patterns matched consumed topic [" + topic + "] for listener [" + method.getDeclaringType().getName() + ']');
                }
                matchedMethod = patternMethod.method;
            }
        }
        if (matchedMethod != null) {
            return matchedMethod;
        }
        if (listenerMethods.size() == 1) {
            return method;
        }
        throw new MessagingSystemException("No @Topic method found for consumed topic [" + topic + "] in listener [" + method.getDeclaringType().getName() + "]");
    }

    private void registerTopicMethods(List<PatternMethod> patterns, ExecutableMethod<Object, ?> executableMethod) {
        for (AnnotationValue<Topic> topicAnnotation : topicAnnotations(executableMethod)) {
            registerDirectTopics(executableMethod, topicAnnotation);
            registerPatterns(patterns, executableMethod, topicAnnotation);
        }
    }

    private void registerDirectTopics(ExecutableMethod<Object, ?> executableMethod, AnnotationValue<Topic> topicAnnotation) {
        for (String topic : topicAnnotation.stringValues()) {
            registerDirectTopic(topic, executableMethod);
            if (nonBlockingRetryTopics != null) {
                for (String retryTopic : nonBlockingRetryTopics.expandTopics(new String[] { topic })) {
                    if (!retryTopic.equals(topic)) {
                        registerDirectTopic(retryTopic, executableMethod);
                    }
                }
            }
        }
    }

    private void registerDirectTopic(String topic, ExecutableMethod<Object, ?> executableMethod) {
        ExecutableMethod<Object, ?> previous = topicMethods.putIfAbsent(topic, executableMethod);
        if (previous != null && previous != executableMethod) {
            throw new MessagingSystemException("Duplicate topic [" + topic + "] found for listener [" + executableMethod.getDeclaringType().getName() + ']');
        }
    }

    private static void registerPatterns(List<PatternMethod> patterns, ExecutableMethod<Object, ?> executableMethod, AnnotationValue<Topic> topicAnnotation) {
        for (String pattern : topicAnnotation.stringValues("patterns")) {
            try {
                patterns.add(new PatternMethod(Pattern.compile(pattern), executableMethod));
            } catch (PatternSyntaxException e) {
                throw new MessagingSystemException("Invalid @Topic pattern [" + pattern + "] for listener method [" + executableMethod.getDeclaringType().getName() + "#" + executableMethod.getName() + "]: " + e.getMessage(), e);
            }
        }
    }

    private record PatternMethod(Pattern pattern, ExecutableMethod<Object, ?> method) {
    }
}
