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
import io.micronaut.configuration.kafka.ConsumerAware;
import io.micronaut.configuration.kafka.ConsumerRegistry;
import io.micronaut.configuration.kafka.ConsumerSeekAware;
import io.micronaut.configuration.kafka.ProducerRegistry;
import io.micronaut.configuration.kafka.TransactionalProducerRegistry;
import io.micronaut.configuration.kafka.annotation.ConsumerCreationStrategy;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.bind.ConsumerRecordBinderRegistry;
import io.micronaut.configuration.kafka.bind.batch.BatchConsumerRecordsBinderRegistry;
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration;
import io.micronaut.configuration.kafka.config.DefaultKafkaConsumerConfiguration;
import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.configuration.kafka.event.KafkaConsumerStartedPollingEvent;
import io.micronaut.configuration.kafka.event.KafkaConsumerSubscribedEvent;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler;
import io.micronaut.configuration.kafka.retry.ConditionalRetryBehaviourHandler;
import io.micronaut.configuration.kafka.seek.KafkaSeeker;
import io.micronaut.configuration.kafka.serde.SerdeRegistry;
import io.micronaut.context.BeanProvider;
import io.micronaut.configuration.kafka.scope.KafkaCustomScope;
import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.bind.annotation.Bindable;
import io.micronaut.core.naming.NameUtils;
import io.micronaut.core.order.OrderUtil;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.ArgumentUtils;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.core.util.SupplierUtil;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.messaging.exceptions.MessagingSystemException;
import io.micronaut.runtime.ApplicationConfiguration;
import io.micronaut.runtime.graceful.GracefulShutdownCapable;
import io.micronaut.scheduling.ScheduledExecutorTaskScheduler;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.TaskScheduler;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * <p>A {@link ExecutableMethodProcessor} that will process all beans annotated with {@link KafkaListener}
 * and create and subscribe the relevant methods as consumers to Kafka topics.</p>
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Singleton
@Requires(beans = KafkaDefaultConfiguration.class)
@Internal
class KafkaConsumerProcessor
        implements ExecutableMethodProcessor<Topic>, AutoCloseable, ConsumerRegistry, GracefulShutdownCapable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerProcessor.class);
    private static final ByteArrayDeserializer DEFAULT_KEY_DESERIALIZER = new ByteArrayDeserializer();
    private static final StringDeserializer DEFAULT_VALUE_DESERIALIZER = new StringDeserializer();

    private final ExecutorService executorService;
    private final ApplicationConfiguration applicationConfiguration;
    private final BeanProvider<KafkaConsumerGroupManager> kafkaConsumerGroupManager;
    private final BeanContext beanContext;
    @SuppressWarnings("rawtypes")
    private final AbstractKafkaConsumerConfiguration defaultConsumerConfiguration;
    private final Map<String, ConsumerState> consumers = new ConcurrentHashMap<>();
    private final Set<Class<?>> perClassConsumers = ConcurrentHashMap.newKeySet();

    private final ConsumerRecordBinderRegistry binderRegistry;
    private final List<ConsumerRecordInterceptor<?, ?>> consumerRecordInterceptors;
    private final SerdeRegistry serdeRegistry;
    private final KafkaListenerExceptionHandler exceptionHandler;
    private final TaskScheduler taskScheduler;
    private final ProducerRegistry producerRegistry;
    private final TransactionalProducerRegistry transactionalProducerRegistry;
    private final BatchConsumerRecordsBinderRegistry batchBinderRegistry;
    private final AtomicInteger clientIdGenerator = new AtomicInteger(10);
    private final ApplicationEventPublisher<KafkaConsumerStartedPollingEvent> kafkaConsumerStartedPollingEventPublisher;
    private final ApplicationEventPublisher<KafkaConsumerSubscribedEvent> kafkaConsumerSubscribedEventPublisher;
    private final ConditionalRetryBehaviourHandler conditionalRetryBehaviourHandler;

    private final Supplier<Optional<KafkaCustomScope>> kafkaCustomScopeSupplier;

    /**
     * Creates a new processor using the given {@link ExecutorService} to schedule consumers on.
     *
     * @param executorService               The executor service
     * @param applicationConfiguration      The application configuration
     * @param kafkaConsumerGroupManager     The {@link KafkaConsumerGroupManager}
     * @param beanContext                   The bean context
     * @param defaultConsumerConfiguration  The default consumer config
     * @param binderRegistry                The {@link ConsumerRecordBinderRegistry}
     * @param consumerRecordInterceptors    Interceptors applied to consumed records before listener binding
     * @param batchBinderRegistry           The {@link BatchConsumerRecordsBinderRegistry}
     * @param serdeRegistry                 The {@link org.apache.kafka.common.serialization.Serde} registry
     * @param producerRegistry              The {@link ProducerRegistry}
     * @param exceptionHandler              The exception handler to use
     * @param schedulerService              The scheduler service
     * @param transactionalProducerRegistry The transactional producer registry
     * @param startedEventPublisher         The KafkaConsumerStartedPollingEvent publisher
     * @param subscribedEventPublisher      The KafkaConsumerSubscribedEvent publisher
     */
    @SuppressWarnings({"rawtypes", "checkstyle:ParameterNumber"})
    KafkaConsumerProcessor(
            @Named(TaskExecutors.MESSAGE_CONSUMER) ExecutorService executorService,
            ApplicationConfiguration applicationConfiguration,
            BeanProvider<KafkaConsumerGroupManager> kafkaConsumerGroupManager,
            BeanContext beanContext,
            AbstractKafkaConsumerConfiguration defaultConsumerConfiguration,
            ConsumerRecordBinderRegistry binderRegistry,
            List<ConsumerRecordInterceptor<?, ?>> consumerRecordInterceptors,
            BatchConsumerRecordsBinderRegistry batchBinderRegistry,
            SerdeRegistry serdeRegistry,
            ProducerRegistry producerRegistry,
            KafkaListenerExceptionHandler exceptionHandler,
            @Named(TaskExecutors.SCHEDULED) ExecutorService schedulerService,
            TransactionalProducerRegistry transactionalProducerRegistry,
            ApplicationEventPublisher<KafkaConsumerStartedPollingEvent> startedEventPublisher,
            ApplicationEventPublisher<KafkaConsumerSubscribedEvent> subscribedEventPublisher,
            ConditionalRetryBehaviourHandler conditionalRetryBehaviourHandler) {
        this.executorService = executorService;
        this.applicationConfiguration = applicationConfiguration;
        this.kafkaConsumerGroupManager = kafkaConsumerGroupManager;
        this.beanContext = beanContext;
        this.defaultConsumerConfiguration = defaultConsumerConfiguration;
        this.binderRegistry = binderRegistry;
        this.consumerRecordInterceptors = new ArrayList<>(consumerRecordInterceptors);
        OrderUtil.sort(this.consumerRecordInterceptors);
        this.batchBinderRegistry = batchBinderRegistry;
        this.serdeRegistry = serdeRegistry;
        this.producerRegistry = producerRegistry;
        this.exceptionHandler = exceptionHandler;
        this.taskScheduler = new ScheduledExecutorTaskScheduler(schedulerService);
        this.transactionalProducerRegistry = transactionalProducerRegistry;
        this.kafkaConsumerStartedPollingEventPublisher = startedEventPublisher;
        this.kafkaConsumerSubscribedEventPublisher = subscribedEventPublisher;
        this.conditionalRetryBehaviourHandler = conditionalRetryBehaviourHandler;
        this.kafkaCustomScopeSupplier = SupplierUtil.memoized(() -> beanContext.findBean(KafkaCustomScope.class));
        this.beanContext.getBeanDefinitions(Qualifiers.byType(KafkaListener.class))
                .forEach(definition -> {
                    // pre-initialize singletons before processing
                    if (definition.isSingleton()) {
                        try {
                            beanContext.getBean(definition.getBeanType());
                        } catch (Exception e) {
                            throw new MessagingSystemException(
                                    "Error creating bean for @KafkaListener of type [" + definition.getBeanType() + "]: " + e.getMessage(),
                                    e
                            );
                        }
                    }
                });
    }

    @NonNull
    private ConsumerState getConsumerState(@NonNull String id) {
        ConsumerState consumerState = consumers.get(id);
        if (consumerState == null) {
            throw new IllegalArgumentException("No consumer found for ID: " + id);
        }
        return consumerState;
    }

    @NonNull
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Consumer<K, V> getConsumer(@NonNull String id) {
        ArgumentUtils.requireNonNull("id", id);
        @SuppressWarnings("rawtypes")
        final Consumer consumer = getConsumerState(id).kafkaConsumer;
        if (consumer == null) {
            throw new IllegalArgumentException("No consumer found for ID: " + id);
        }
        return consumer;
    }

    @NonNull
    @Override
    public Set<String> getConsumerSubscription(@NonNull final String id) {
        ArgumentUtils.requireNonNull("id", id);
        final Set<String> subscriptions = getConsumerState(id).subscriptions;
        if (subscriptions == null || subscriptions.isEmpty()) {
            throw new IllegalArgumentException("No consumer subscription found for ID: " + id);
        }
        return subscriptions;
    }

    @NonNull
    @Override
    public Set<TopicPartition> getConsumerAssignment(@NonNull final String id) {
        ArgumentUtils.requireNonNull("id", id);
        final Set<TopicPartition> assignment = getConsumerState(id).assignments;
        if (assignment == null || assignment.isEmpty()) {
            throw new IllegalArgumentException("No consumer assignment found for ID: " + id);
        }
        return assignment;
    }

    @NonNull
    @Override
    public Set<String> getConsumerIds() {
        return Collections.unmodifiableSet(consumers.keySet());
    }

    @Override
    public boolean isPaused(@NonNull String id) {
        return isPaused(id, getConsumerState(id).assignments);
    }

    @Override
    public boolean isPaused(@NonNull String id, @NonNull Collection<TopicPartition> topicPartitions) {
        return getConsumerState(id).isPaused(topicPartitions);
    }

    @Override
    public void pause(@NonNull String id) {
        getConsumerState(id).pause();
    }

    @Override
    public void pause(@NonNull String id, @NonNull Collection<TopicPartition> topicPartitions) {
        getConsumerState(id).pause(topicPartitions);
    }

    @Override
    public void resume(@NonNull String id) {
        getConsumerState(id).resume();
    }

    @Override
    public void resume(@NonNull String id, @NonNull Collection<TopicPartition> topicPartitions) {
        getConsumerState(id).resume(topicPartitions);
    }

    @Override
    public <B> void process(BeanDefinition<B> beanDefinition, ExecutableMethod<B, ?> method) {
        final AnnotationValue<KafkaListener> consumerAnnotation = method.getAnnotation(KafkaListener.class);
        if (consumerAnnotation == null) {
            return;
        }

        final ConsumerCreationStrategy consumerCreationStrategy = consumerAnnotation.enumValue("consumerCreationStrategy", ConsumerCreationStrategy.class)
            .orElse(ConsumerCreationStrategy.PER_TOPIC);
        final Class<?> beanType = beanDefinition.getBeanType();
        if (consumerCreationStrategy == ConsumerCreationStrategy.PER_CLASS && !perClassConsumers.add(beanType)) {
            return;
        }

        final List<ExecutableMethod<?, ?>> methods = resolveConsumerMethods(beanDefinition, method, consumerCreationStrategy);
        final List<AnnotationValue<Topic>> topicAnnotations = resolveTopicAnnotations(beanDefinition, method, consumerCreationStrategy, methods);
        if (CollectionUtils.isEmpty(topicAnnotations)) {
            return; // No topics to consume
        }
        final Optional<String> listenerId = consumerAnnotation.stringValue("id")
            .filter(StringUtils::isNotEmpty);
        final Optional<String> annotationGroupId = consumerAnnotation.stringValue("groupId")
            .filter(StringUtils::isNotEmpty);
        final String clientId = consumerAnnotation.stringValue("clientId")
                .filter(StringUtils::isNotEmpty)
                .orElseGet(() -> applicationConfiguration.getName().map(s -> s + '-' + NameUtils.hyphenate(beanType.getSimpleName())).orElse(null));
        final OffsetStrategy offsetStrategy = consumerAnnotation.enumValue("offsetStrategy", OffsetStrategy.class)
                .orElse(OffsetStrategy.AUTO);
        final String defaultId = applicationConfiguration.getName().orElse(beanType.getName());
        final String fallbackGroupId = annotationGroupId
            .or(() -> listenerId)
            .orElse(defaultId);
        final String configId = listenerId
            .or(() -> annotationGroupId)
            .orElse(defaultId);
        final AbstractKafkaConsumerConfiguration<?, ?> consumerConfigurationDefaults = getConsumerConfigurationDefaults(configId);
        boolean uniqueGroupIdDeleteOnShutdown = false;
        final DefaultKafkaConsumerConfiguration<?, ?> consumerConfiguration = new DefaultKafkaConsumerConfiguration<>(consumerConfigurationDefaults);
        final Properties properties = createConsumerProperties(
            consumerAnnotation,
            consumerConfiguration,
            clientId,
            fallbackGroupId,
            annotationGroupId.isPresent(),
            offsetStrategy
        );
        String groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG, fallbackGroupId);
        if (consumerAnnotation.isTrue("uniqueGroupId")) {
            groupId = groupId + "_" + UUID.randomUUID();
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            if (consumerAnnotation.isTrue("uniqueGroupIdDeleteOnShutdown")) {
                uniqueGroupIdDeleteOnShutdown = true;
            }
        }
        final ExecutableMethod<?, ?> primaryMethod = methods.get(0);
        final boolean batch = primaryMethod.isTrue(KafkaListener.class, "batch");
        final NonBlockingRetryTopics nonBlockingRetryTopics = NonBlockingRetryTopics.create(consumerAnnotation, topicAnnotations, batch);
        configureDeserializers(methods, consumerConfiguration, nonBlockingRetryTopics);
        submitConsumerThreads(beanDefinition, primaryMethod, clientId, groupId, offsetStrategy, topicAnnotations,
            consumerAnnotation, consumerConfiguration, properties, beanType, methods, uniqueGroupIdDeleteOnShutdown);
    }

    @Override
    public CompletableFuture<Void> shutdownGracefully() {
        List<ConsumerState> consumerStates = List.copyOf(consumers.values());
        if (consumerStates.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        consumerStates.forEach(ConsumerState::requestShutdown);
        consumerStates.forEach(ConsumerState::wakeUp);
        return CompletableFuture.allOf(consumerStates.stream()
            .map(ConsumerState::getShutdownFuture)
            .toArray(CompletableFuture[]::new));
    }

    @Override
    public OptionalLong reportActiveTasks() {
        return OptionalLong.of(consumers.values().stream()
            .filter(ConsumerState::isActive)
            .count());
    }

    @Override
    @PreDestroy
    public void close() {
        consumers.values().forEach(ConsumerState::requestShutdown);
        consumers.values().forEach(state -> {
            if (state.isActive()) {
                state.wakeUp();
            }
            state.close();
        });
        consumers.clear();
    }

    void publishStartedPollingEvent(Consumer<?, ?> consumer) {
        kafkaConsumerStartedPollingEventPublisher.publishEvent(new KafkaConsumerStartedPollingEvent(consumer));
    }

    void handleException(Object consumerBean, KafkaListenerException kafkaListenerException) {
        try {
            if (consumerBean instanceof KafkaListenerExceptionHandler kle) {
                kle.handle(kafkaListenerException);
            } else {
                exceptionHandler.handle(kafkaListenerException);
            }
        } catch (Exception e) {
            // The exception handler could not handle the consumer exception
            // Log both errors and continue as usual to prevent an infinite loop
            e.addSuppressed(kafkaListenerException);
            LOG.error("Unexpected error while handling the kafka listener exception", e);
        }
    }

    boolean shouldRetryMessage(Object consumerBean, KafkaListenerException kafkaListenerException) {
        final ConditionalRetryBehaviourHandler conditionalRetryBehaviourHandler;
        if (consumerBean instanceof ConditionalRetryBehaviourHandler kle) {
            conditionalRetryBehaviourHandler = kle;
        } else {
            conditionalRetryBehaviourHandler = this.conditionalRetryBehaviourHandler;
        }

        try {
            return conditionalRetryBehaviourHandler.conditionalRetryBehaviour(kafkaListenerException) == ConditionalRetryBehaviourHandler.ConditionalRetryBehaviour.RETRY;
        } catch (Exception e) {
            // The behaviour exception handler could not handle the consumer exception
            // Log both errors and continue as usual to prevent an infinite loop
            e.addSuppressed(kafkaListenerException);
            LOG.error("Unexpected error while determining how to handle the kafka listener exception", e);
        }
        return false;
    }

    void scheduleTask(Duration delay, Runnable command) {
        taskScheduler.schedule(delay, command);
    }

    <K, V> Producer<K, V> getProducer(String id, Class<K> keyType, Class<V> valueType) {
        return producerRegistry.getProducer(id, Argument.of(keyType), Argument.of(valueType));
    }

    <K, V> Producer<K, V> getTransactionalProducer(@Nullable String clientId, @Nullable String transactionalId, Class<K> keyClass, Class<V> valueClass) {
        if (transactionalId == null) {
            throw new IllegalStateException("Transactional id is required to create a transactional Kafka producer");
        }
        return transactionalProducerRegistry.getTransactionalProducer(clientId, transactionalId, Argument.of(keyClass), Argument.of(valueClass));
    }

    void handleProducerFencedException(Producer<?, ?> producer, ProducerFencedException e) {
        LOG.error("Failed accessing the producer: {}", producer, e);
        transactionalProducerRegistry.close(producer);
    }

    @SuppressWarnings("unchecked")
    <T> Flux<T> convertPublisher(T result) {
        return Flux.from((Publisher<T>) Publishers.convertPublisher(beanContext.getConversionService(), result, Publisher.class));
    }

    ConsumerRecordBinderRegistry getBinderRegistry() {
        return binderRegistry;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    <K, V> ConsumerRecord<K, V> interceptRecord(@NonNull ConsumerInfo consumerInfo, @NonNull ConsumerRecord<K, V> consumerRecord) {
        List<ConsumerRecordInterceptor<?, ?>> applicableInterceptors = consumerInfo.consumerRecordInterceptors(consumerRecord.topic());
        if (applicableInterceptors.isEmpty()) {
            return consumerRecord;
        }
        ConsumerRecord<K, V> intercepted = consumerRecord;
        for (ConsumerRecordInterceptor<?, ?> consumerRecordInterceptor : applicableInterceptors) {
            ConsumerRecordInterceptor.InterceptionContext<K, V> interceptionContext = new ConsumerRecordInterceptor.InterceptionContext<>(
                intercepted,
                intercepted.topic(),
                intercepted.partition(),
                intercepted.offset(),
                consumerInfo.clientId,
                consumerInfo.groupId
            );
            intercepted = ((ConsumerRecordInterceptor<K, V>) consumerRecordInterceptor).intercept(interceptionContext);
            validateCoordinates(consumerRecord, intercepted);
            if (intercepted == null) {
                return null;
            }
        }
        return intercepted;
    }

    @NonNull
    @SuppressWarnings("unchecked")
    <K, V> ConsumerRecords<K, V> interceptRecords(@NonNull ConsumerInfo consumerInfo, @NonNull ConsumerRecords<K, V> consumerRecords) {
        if (consumerRecords.isEmpty()) {
            return consumerRecords;
        }
        Map<TopicPartition, List<ConsumerRecord<K, V>>> interceptedRecords = new LinkedHashMap<>();
        for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
            ConsumerRecord<K, V> intercepted = interceptRecord(consumerInfo, consumerRecord);
            if (intercepted != null) {
                TopicPartition topicPartition = new TopicPartition(intercepted.topic(), intercepted.partition());
                interceptedRecords.computeIfAbsent(topicPartition, ignored -> new ArrayList<>()).add(intercepted);
            }
        }
        if (interceptedRecords.isEmpty()) {
            return ConsumerRecords.empty();
        }
        return new ConsumerRecords<>(interceptedRecords);
    }

    private static void validateCoordinates(ConsumerRecord<?, ?> original, @Nullable ConsumerRecord<?, ?> intercepted) {
        if (intercepted == null) {
            return;
        }
        if (!original.topic().equals(intercepted.topic()) ||
            original.partition() != intercepted.partition() ||
            original.offset() != intercepted.offset()) {
            throw new IllegalStateException("ConsumerRecordInterceptor must preserve the consumed record topic, partition, and offset");
        }
    }

    List<ConsumerRecordInterceptor<?, ?>> matchingInterceptors(@NonNull BeanDefinition<?> beanDefinition, @NonNull ExecutableMethod<?, ?> method) {
        if (consumerRecordInterceptors.isEmpty()) {
            return List.of();
        }
        return consumerRecordInterceptors.stream()
            .filter(interceptor -> interceptor.matches(beanDefinition, method))
            .toList();
    }

    Map<ExecutableMethod<?, ?>, List<ConsumerRecordInterceptor<?, ?>>> matchingInterceptors(
        @NonNull BeanDefinition<?> beanDefinition,
        @NonNull List<ExecutableMethod<?, ?>> methods
    ) {
        if (methods.isEmpty()) {
            return Map.of();
        }
        Map<ExecutableMethod<?, ?>, List<ConsumerRecordInterceptor<?, ?>>> matches = new LinkedHashMap<>(methods.size());
        for (ExecutableMethod<?, ?> executableMethod : methods) {
            matches.put(executableMethod, matchingInterceptors(beanDefinition, executableMethod));
        }
        return Map.copyOf(matches);
    }

    BatchConsumerRecordsBinderRegistry getBatchBinderRegistry() {
        return batchBinderRegistry;
    }

    @Nullable
    KafkaCustomScope getKafkaScope() {
        return kafkaCustomScopeSupplier.get().orElse(null);
    }

    private static List<ExecutableMethod<?, ?>> resolveConsumerMethods(
        BeanDefinition<?> beanDefinition,
        ExecutableMethod<?, ?> method,
        ConsumerCreationStrategy consumerCreationStrategy
    ) {
        if (consumerCreationStrategy == ConsumerCreationStrategy.PER_CLASS &&
            CollectionUtils.isEmpty(beanDefinition.getDeclaredAnnotationValuesByType(Topic.class))) {
            final List<ExecutableMethod<?, ?>> methods = beanDefinition.getExecutableMethods().stream()
                .filter(executableMethod -> !CollectionUtils.isEmpty(executableMethod.getDeclaredAnnotationValuesByType(Topic.class)))
                .<ExecutableMethod<?, ?>>map(executableMethod -> executableMethod)
                .toList();
            if (CollectionUtils.isNotEmpty(methods)) {
                return methods;
            }
        }
        return Collections.singletonList(method);
    }

    private static List<AnnotationValue<Topic>> resolveTopicAnnotations(
        BeanDefinition<?> beanDefinition,
        ExecutableMethod<?, ?> method,
        ConsumerCreationStrategy consumerCreationStrategy,
        List<ExecutableMethod<?, ?>> methods
    ) {
        if (consumerCreationStrategy == ConsumerCreationStrategy.PER_CLASS && methods.size() > 1) {
            return methods.stream()
                .flatMap(executableMethod -> executableMethod.getDeclaredAnnotationValuesByType(Topic.class).stream())
                .toList();
        }
        final List<AnnotationValue<Topic>> methodTopics = method.getDeclaredAnnotationValuesByType(Topic.class);
        if (CollectionUtils.isNotEmpty(methodTopics)) {
            return methodTopics;
        }
        return beanDefinition.getDeclaredAnnotationValuesByType(Topic.class);
    }

    @SuppressWarnings("rawtypes")
    private AbstractKafkaConsumerConfiguration getConsumerConfigurationDefaults(String groupId) {
        return findConfigurationBean(groupId)
            .or(() -> findHyphenatedConsumerConfigurationBean(groupId))
            .orElse(defaultConsumerConfiguration);
    }

    @SuppressWarnings("rawtypes")
    private Optional<AbstractKafkaConsumerConfiguration> findConfigurationBean(String groupId) {
        return beanContext.findBean(AbstractKafkaConsumerConfiguration.class, Qualifiers.byName(groupId));
    }

    @SuppressWarnings("rawtypes")
    private Optional<AbstractKafkaConsumerConfiguration> findHyphenatedConsumerConfigurationBean(String groupId) {
        if (NameUtils.isValidHyphenatedPropertyName(groupId)) {
            return Optional.empty();
        }
        return findConfigurationBean(NameUtils.hyphenate(groupId));
    }

    @SuppressWarnings("rawtypes")
    private Properties createConsumerProperties(final AnnotationValue<KafkaListener> consumerAnnotation,
                                                final DefaultKafkaConsumerConfiguration consumerConfiguration,
                                                final String clientId,
                                                final String groupId,
                                                final boolean overrideGroupId,
                                                final OffsetStrategy offsetStrategy) {
        final Properties properties = consumerConfiguration.getConfig();

        if (consumerAnnotation.getRequiredValue("offsetReset", OffsetReset.class) == OffsetReset.EARLIEST) {
            properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetReset.EARLIEST.name().toLowerCase());
        }

        // enable auto commit offsets if necessary
        properties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(offsetStrategy == OffsetStrategy.AUTO));

        consumerAnnotation.get("heartbeatInterval", Duration.class)
                .map(Duration::toMillis)
                .map(String::valueOf)
                .ifPresent(heartbeatInterval -> properties.putIfAbsent(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval));

        consumerAnnotation.get("sessionTimeout", Duration.class)
                .map(Duration::toMillis)
                .map(String::valueOf)
                .ifPresent(sessionTimeout -> properties.putIfAbsent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout));

        consumerAnnotation.enumValue("isolation", IsolationLevel.class)
                .ifPresent(isolation -> properties.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation.toString().toLowerCase(Locale.ROOT)));

        if (overrideGroupId) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        } else {
            properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        if (clientId != null) {
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }

        properties.putAll(consumerAnnotation.getProperties("properties", "name"));
        return properties;
    }

    private void debugDeserializationConfiguration(final ExecutableMethod<?, ?> method, final DefaultKafkaConsumerConfiguration<?, ?> consumerConfiguration) {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        final Properties properties = consumerConfiguration.getConfig();
        final String logMethod = logMethod(method);
        final String keyDeserializerClass = consumerConfiguration.getKeyDeserializer()
            .map(Object::toString)
            .orElseGet(() -> properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        final String valueDeserializerClass = consumerConfiguration.getValueDeserializer()
            .map(Object::toString)
            .orElseGet(() -> properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        LOG.debug("Using key deserializer [{}] for Kafka listener: {}", keyDeserializerClass, logMethod);
        LOG.debug("Using value deserializer [{}] for Kafka listener: {}", valueDeserializerClass, logMethod);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void submitConsumerThreads(final BeanDefinition<?> beanDefinition,
                                       final ExecutableMethod<?, ?> method,
                                       final String clientId,
                                       final String groupId,
                                       final OffsetStrategy offsetStrategy,
                                       final List<AnnotationValue<Topic>> topicAnnotations,
                                       final AnnotationValue<KafkaListener> consumerAnnotation,
                                       final DefaultKafkaConsumerConfiguration<?, ?> consumerConfiguration,
                                       final Properties properties,
                                       final Class<?> beanType,
                                       final List<ExecutableMethod<?, ?>> methods,
                                       boolean uniqueGroupIdDeleteOnShutdown) {
        final int consumerThreads = consumerAnnotation.intValue("threads").orElse(1);
        for (int i = 0; i < consumerThreads; i++) {
            final String finalClientId;
            if (clientId != null) {
                if (consumerThreads > 1 || consumers.containsKey(clientId)) {
                    finalClientId = clientId + '-' + clientIdGenerator.incrementAndGet();
                } else {
                    finalClientId = clientId;
                }
                properties.put(ConsumerConfig.CLIENT_ID_CONFIG, finalClientId);
            } else {
                finalClientId = "kafka-consumer-" + clientIdGenerator.incrementAndGet();
            }
            final Consumer<?, ?> kafkaConsumer = beanContext.createBean(Consumer.class, consumerConfiguration);
            final Object consumerBean = beanContext.getBean(beanType);
            if (consumerBean instanceof ConsumerAware ca) {
                //noinspection unchecked
                ca.setKafkaConsumer(kafkaConsumer);
            }
            final ConsumerInfo consumerInfo = new ConsumerInfo(
                finalClientId,
                groupId,
                offsetStrategy,
                consumerAnnotation,
                properties,
                methods,
                topicAnnotations,
                matchingInterceptors(beanDefinition, methods)
            );
            setupConsumerSubscription(method, topicAnnotations, consumerInfo, consumerBean, kafkaConsumer);
            kafkaConsumerSubscribedEventPublisher.publishEvent(new KafkaConsumerSubscribedEvent(kafkaConsumer));
            final ConsumerState consumerState = consumerInfo.isBatch ?
                new ConsumerStateBatch(this, consumerInfo, kafkaConsumer, consumerBean) :
                new ConsumerStateSingle(this, consumerInfo, kafkaConsumer, consumerBean);
            consumers.put(finalClientId, consumerState);
            if (uniqueGroupIdDeleteOnShutdown) {
                KafkaConsumerGroupManager consumerGroupManager = kafkaConsumerGroupManager.get();
                consumerGroupManager.registerConsumerForGroupDeletion(finalClientId, consumerState);
                consumerGroupManager.registerConsumerGroupIdForDeletion(groupId);
            }
            executorService.submit(consumerState::threadPollLoop);
        }
    }

    private static void setupConsumerSubscription(
        ExecutableMethod<?, ?> method,
        List<AnnotationValue<Topic>> topicAnnotations,
        ConsumerInfo consumerInfo,
        Object consumerBean,
        Consumer<?, ?> kafkaConsumer
    ) {
        java.util.stream.Stream<String> directTopics = topicAnnotations.stream()
            .flatMap(annotationValue -> Arrays.stream(annotationValue.stringValues()))
            .filter(StringUtils::isNotEmpty);
        if (consumerInfo.nonBlockingRetryTopics != null) {
            directTopics = java.util.stream.Stream.concat(directTopics, consumerInfo.nonBlockingRetryTopics.additionalRetryTopics().stream());
        }
        final List<String> topicNames = directTopics.collect(java.util.stream.Collectors.collectingAndThen(
            java.util.stream.Collectors.toCollection(LinkedHashSet::new),
            List::copyOf
        ));
        final List<String> patterns = topicAnnotations.stream()
            .flatMap(annotationValue -> Arrays.stream(annotationValue.stringValues("patterns")))
            .collect(java.util.stream.Collectors.collectingAndThen(
                java.util.stream.Collectors.toCollection(LinkedHashSet::new),
                List::copyOf
            ));
        final boolean hasTopics = !topicNames.isEmpty();
        final boolean hasPatterns = !patterns.isEmpty();
        final String logMethod = LOG.isInfoEnabled() ? logMethod(method) : null;

        if (!hasTopics && !hasPatterns) {
            throw new MessagingSystemException("Either topics or topic patterns must be specified for method: " + method);
        }

        final Optional<ConsumerRebalanceListener> listener = getConsumerRebalanceListener(consumerBean, kafkaConsumer);

        if (hasPatterns) {
            try {
                final Pattern compiledPattern = compileTopicPattern(topicNames, patterns);
                listener.ifPresentOrElse(
                    l -> kafkaConsumer.subscribe(compiledPattern, l),
                    () -> kafkaConsumer.subscribe(compiledPattern));
                LOG.info("Kafka listener [{}] subscribed to topics: {} and topic patterns: {}", logMethod, topicNames, patterns);
            } catch (PatternSyntaxException e) {
                throw new MessagingSystemException("Invalid topic pattern [" + e.getPattern() + "] for method [" + method + "]: " + e.getMessage(), e);
            }
            return;
        }

        listener.ifPresentOrElse(
            l -> kafkaConsumer.subscribe(topicNames, l),
            () -> kafkaConsumer.subscribe(topicNames));
        LOG.info("Kafka listener [{}] subscribed to topics: {}", logMethod, topicNames);
    }

    @SuppressWarnings("unused")
    private static void setupConsumerSubscription(
        ExecutableMethod<?, ?> method,
        List<AnnotationValue<Topic>> topicAnnotations,
        Object consumerBean,
        Consumer<?, ?> kafkaConsumer
    ) {
        java.util.stream.Stream<String> directTopics = topicAnnotations.stream()
            .flatMap(annotationValue -> Arrays.stream(annotationValue.stringValues()))
            .filter(StringUtils::isNotEmpty);
        final List<String> topicNames = directTopics.collect(java.util.stream.Collectors.collectingAndThen(
            java.util.stream.Collectors.toCollection(LinkedHashSet::new),
            List::copyOf
        ));
        final List<String> patterns = topicAnnotations.stream()
            .flatMap(annotationValue -> Arrays.stream(annotationValue.stringValues("patterns")))
            .collect(java.util.stream.Collectors.collectingAndThen(
                java.util.stream.Collectors.toCollection(LinkedHashSet::new),
                List::copyOf
            ));
        final boolean hasTopics = !topicNames.isEmpty();
        final boolean hasPatterns = !patterns.isEmpty();
        final String logMethod = LOG.isInfoEnabled() ? logMethod(method) : null;

        if (!hasTopics && !hasPatterns) {
            throw new MessagingSystemException("Either topics or topic patterns must be specified for method: " + method);
        }

        final Optional<ConsumerRebalanceListener> listener = getConsumerRebalanceListener(consumerBean, kafkaConsumer);

        if (hasPatterns) {
            try {
                final Pattern compiledPattern = compileTopicPattern(topicNames, patterns);
                listener.ifPresentOrElse(
                    l -> kafkaConsumer.subscribe(compiledPattern, l),
                    () -> kafkaConsumer.subscribe(compiledPattern));
                LOG.info("Kafka listener [{}] subscribed to topics: {} and topic patterns: {}", logMethod, topicNames, patterns);
            } catch (PatternSyntaxException e) {
                throw new MessagingSystemException("Invalid topic pattern [" + e.getPattern() + "] for method [" + method + "]: " + e.getMessage(), e);
            }
            return;
        }

        listener.ifPresentOrElse(
            l -> kafkaConsumer.subscribe(topicNames, l),
            () -> kafkaConsumer.subscribe(topicNames));
        LOG.info("Kafka listener [{}] subscribed to topics: {}", logMethod, topicNames);
    }

    private static Pattern compileTopicPattern(List<String> topicNames, List<String> patterns) {
        final String joinedPattern = java.util.stream.Stream.concat(
                topicNames.stream().map(Pattern::quote),
                patterns.stream().map(pattern -> "(?:" + pattern + ')')
            )
            .collect(java.util.stream.Collectors.joining("|"));
        return Pattern.compile(joinedPattern);
    }

    private static Optional<ConsumerRebalanceListener> getConsumerRebalanceListener(Object consumerBean, Consumer<?, ?> kafkaConsumer) {
        if (consumerBean instanceof ConsumerSeekAware csa) {
            return Optional.of(new ConsumerSeekAwareAdapter(KafkaSeeker.newInstance(kafkaConsumer), csa));
        }
        if (consumerBean instanceof ConsumerRebalanceListener crl) {
            return Optional.of(crl);
        }
        return Optional.empty();
    }

    private static Argument<?> findBodyArgument(ExecutableMethod<?, ?> method) {
        return Arrays.stream(method.getArguments())
                .filter(arg -> isConsumerRecord(arg) || arg.getAnnotationMetadata().hasAnnotation(MessageBody.class))
                .findFirst()
                .orElseGet(() -> Arrays.stream(method.getArguments())
                        .filter(arg -> !arg.getAnnotationMetadata().hasStereotype(Bindable.class)
                            && !isLastArgumentOfSuspendedMethod(arg, method))
                        .findFirst()
                        .orElse(null));
    }

    private static Argument<?> findBodyArgument(boolean batch, ExecutableMethod<?, ?> method) {
        final Argument<?> tempBodyArg = findBodyArgument(method);

        if (batch && tempBodyArg != null) {
            return isConsumerRecord(tempBodyArg) ? tempBodyArg : getComponentType(tempBodyArg);
        }

        return tempBodyArg;
    }

    private static boolean isLastArgumentOfSuspendedMethod(Argument<?> argument, ExecutableMethod<?, ?> method) {
        if (!method.isSuspend()) {
            return false;
        }
        Argument<?> lastArgumentValue = method.getArguments()[method.getArguments().length - 1];
        return argument.equals(lastArgumentValue);
    }

    private void configureDeserializers(
        final List<ExecutableMethod<?, ?>> methods,
        final DefaultKafkaConsumerConfiguration<?, ?> config,
        @Nullable NonBlockingRetryTopics nonBlockingRetryTopics
    ) {
        if (methods.size() == 1) {
            configureDeserializers(methods.get(0), config);
            return;
        }
        if (!config.getConfig().containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) && config.getKeyDeserializer().isEmpty()) {
            config.setKeyDeserializer((Deserializer) new TopicAwareDeserializer(buildDeserializerRouter(methods, true, nonBlockingRetryTopics), "key"));
        }
        if (!config.getConfig().containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) && config.getValueDeserializer().isEmpty()) {
            config.setValueDeserializer((Deserializer) new TopicAwareDeserializer(buildDeserializerRouter(methods, false, nonBlockingRetryTopics), "value"));
        }
        debugDeserializationConfiguration(methods.get(0), config);
    }

    private void configureDeserializers(final ExecutableMethod<?, ?> method, final DefaultKafkaConsumerConfiguration<?, ?> config) {
        final boolean batch = method.isTrue(KafkaListener.class, "batch");
        final Argument<?> bodyArgument = findBodyArgument(batch, method);
        configureKeyDeserializer(bodyArgument, method, config);
        configureValueDeserializer(bodyArgument, config);
        debugDeserializationConfiguration(method, config);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private TopicRouter<Deserializer<Object>> buildDeserializerRouter(
        List<ExecutableMethod<?, ?>> methods,
        boolean key,
        @Nullable NonBlockingRetryTopics nonBlockingRetryTopics
    ) {
        TopicRouter<Deserializer<Object>> router = new TopicRouter<>();
        for (ExecutableMethod<?, ?> method : methods) {
            final boolean batch = method.isTrue(KafkaListener.class, "batch");
            final Argument<?> bodyArgument = findBodyArgument(batch, method);
            final Deserializer<Object> deserializer = key ? resolveKeyDeserializer(bodyArgument, method) : resolveValueDeserializer(bodyArgument);
            for (AnnotationValue<Topic> topicAnnotation : method.getDeclaredAnnotationValuesByType(Topic.class)) {
                String[] topics = topicAnnotation.stringValues();
                if (nonBlockingRetryTopics != null && topics.length > 0) {
                    topics = nonBlockingRetryTopics.expandTopics(topics).toArray(String[]::new);
                }
                router.register(topics, topicAnnotation.stringValues("patterns"), deserializer, logMethod(method));
            }
        }
        return router;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void configureKeyDeserializer(Argument<?> bodyArgument, ExecutableMethod<?, ?> method, DefaultKafkaConsumerConfiguration config) {
        if (!config.getConfig().containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) && config.getKeyDeserializer().isEmpty()) {
            // figure out the Key deserializer
            config.setKeyDeserializer(resolveKeyDeserializer(bodyArgument, method));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void configureValueDeserializer(Argument<?> bodyArgument, DefaultKafkaConsumerConfiguration config) {
        if (!config.getConfig().containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) && config.getValueDeserializer().isEmpty()) {
            // figure out the Value deserializer
            config.setValueDeserializer(resolveValueDeserializer(bodyArgument));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Deserializer<Object> resolveKeyDeserializer(Argument<?> bodyArgument, ExecutableMethod<?, ?> method) {
        Optional<Deserializer<Object>> deserializer = Arrays.stream(method.getArguments())
            .filter(arg -> arg.isAnnotationPresent(KafkaKey.class))
            .findFirst()
            .or(() -> Optional.ofNullable(bodyArgument)
                .filter(KafkaConsumerProcessor::isConsumerRecord)
                .flatMap(b -> b.getTypeVariable("K")))
            .map(argument -> (Deserializer<Object>) serdeRegistry.pickDeserializer(argument));
        return deserializer.orElseGet(KafkaConsumerProcessor::defaultKeyDeserializer);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Deserializer<Object> resolveValueDeserializer(Argument<?> bodyArgument) {
        final Optional<Argument<?>> body = Optional.ofNullable(bodyArgument);
        Optional<Deserializer<Object>> deserializer = body.filter(KafkaConsumerProcessor::isConsumerRecord)
            .flatMap(b -> b.getTypeVariable("V"))
            .or(() -> body)
            .map(argument -> (Deserializer<Object>) serdeRegistry.pickDeserializer(argument));
        return deserializer.orElseGet(KafkaConsumerProcessor::defaultValueDeserializer);
    }

    private static boolean isConsumerRecord(@NonNull Argument<?> body) {
        return ConsumerRecord.class.isAssignableFrom(body.getType()) ||
            ConsumerRecords.class.isAssignableFrom(body.getType());
    }

    @SuppressWarnings("unchecked")
    private static Deserializer<Object> defaultKeyDeserializer() {
        return (Deserializer<Object>) (Deserializer<?>) DEFAULT_KEY_DESERIALIZER;
    }

    @SuppressWarnings("unchecked")
    private static Deserializer<Object> defaultValueDeserializer() {
        return (Deserializer<Object>) (Deserializer<?>) DEFAULT_VALUE_DESERIALIZER;
    }

    private static Argument<?> getComponentType(final Argument<?> argument) {
        final Class<?> argumentType = argument.getType();

        return argumentType.isArray()
                ? Argument.of(argumentType.getComponentType())
                : argument.getFirstTypeVariable().orElse(argument);
    }

    private static String logMethod(ExecutableMethod<?, ?> method) {
        return method.getDeclaringType().getSimpleName() + "#" + method.getName();
    }
}
