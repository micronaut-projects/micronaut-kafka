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

import io.micronaut.configuration.kafka.ConsumerAware;
import io.micronaut.configuration.kafka.ConsumerRegistry;
import io.micronaut.configuration.kafka.ConsumerSeekAware;
import io.micronaut.configuration.kafka.ProducerRegistry;
import io.micronaut.configuration.kafka.TransactionalProducerRegistry;
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
import io.micronaut.configuration.kafka.seek.KafkaSeeker;
import io.micronaut.configuration.kafka.serde.SerdeRegistry;
import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.bind.BoundExecutable;
import io.micronaut.core.bind.DefaultExecutableBinder;
import io.micronaut.core.bind.ExecutableBinder;
import io.micronaut.core.bind.annotation.Bindable;
import io.micronaut.core.naming.NameUtils;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.ArgumentUtils;
import io.micronaut.core.util.ArrayUtils;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.messaging.exceptions.MessagingSystemException;
import io.micronaut.runtime.ApplicationConfiguration;
import io.micronaut.scheduling.ScheduledExecutorTaskScheduler;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.TaskScheduler;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

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
        implements ExecutableMethodProcessor<Topic>, AutoCloseable, ConsumerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerProcessor.class);

    private final ExecutorService executorService;
    private final ApplicationConfiguration applicationConfiguration;
    private final BeanContext beanContext;
    @SuppressWarnings("rawtypes")
    private final AbstractKafkaConsumerConfiguration defaultConsumerConfiguration;
    private final Map<String, ConsumerState> consumers = new ConcurrentHashMap<>();

    private final ConsumerRecordBinderRegistry binderRegistry;
    private final SerdeRegistry serdeRegistry;
    private final KafkaListenerExceptionHandler exceptionHandler;
    private final TaskScheduler taskScheduler;
    private final ProducerRegistry producerRegistry;
    private final TransactionalProducerRegistry transactionalProducerRegistry;
    private final BatchConsumerRecordsBinderRegistry batchBinderRegistry;
    private final AtomicInteger clientIdGenerator = new AtomicInteger(10);
    private final ApplicationEventPublisher<KafkaConsumerStartedPollingEvent> kafkaConsumerStartedPollingEventPublisher;
    private final ApplicationEventPublisher<KafkaConsumerSubscribedEvent> kafkaConsumerSubscribedEventPublisher;

    /**
     * Creates a new processor using the given {@link ExecutorService} to schedule consumers on.
     *
     * @param executorService               The executor service
     * @param applicationConfiguration      The application configuration
     * @param beanContext                   The bean context
     * @param defaultConsumerConfiguration  The default consumer config
     * @param binderRegistry                The {@link ConsumerRecordBinderRegistry}
     * @param batchBinderRegistry           The {@link BatchConsumerRecordsBinderRegistry}
     * @param serdeRegistry                 The {@link org.apache.kafka.common.serialization.Serde} registry
     * @param producerRegistry              The {@link ProducerRegistry}
     * @param exceptionHandler              The exception handler to use
     * @param schedulerService              The scheduler service
     * @param transactionalProducerRegistry The transactional producer registry
     * @param startedEventPublisher         The KafkaConsumerStartedPollingEvent publisher
     * @param subscribedEventPublisher      The KafkaConsumerSubscribedEvent publisher
     */
    @SuppressWarnings("rawtypes")
    KafkaConsumerProcessor(
            @Named(TaskExecutors.MESSAGE_CONSUMER) ExecutorService executorService,
            ApplicationConfiguration applicationConfiguration,
            BeanContext beanContext,
            AbstractKafkaConsumerConfiguration defaultConsumerConfiguration,
            ConsumerRecordBinderRegistry binderRegistry,
            BatchConsumerRecordsBinderRegistry batchBinderRegistry,
            SerdeRegistry serdeRegistry,
            ProducerRegistry producerRegistry,
            KafkaListenerExceptionHandler exceptionHandler,
            @Named(TaskExecutors.SCHEDULED) ExecutorService schedulerService,
            TransactionalProducerRegistry transactionalProducerRegistry,
            ApplicationEventPublisher<KafkaConsumerStartedPollingEvent> startedEventPublisher,
            ApplicationEventPublisher<KafkaConsumerSubscribedEvent> subscribedEventPublisher) {
        this.executorService = executorService;
        this.applicationConfiguration = applicationConfiguration;
        this.beanContext = beanContext;
        this.defaultConsumerConfiguration = defaultConsumerConfiguration;
        this.binderRegistry = binderRegistry;
        this.batchBinderRegistry = batchBinderRegistry;
        this.serdeRegistry = serdeRegistry;
        this.producerRegistry = producerRegistry;
        this.exceptionHandler = exceptionHandler;
        this.taskScheduler = new ScheduledExecutorTaskScheduler(schedulerService);
        this.transactionalProducerRegistry = transactionalProducerRegistry;
        this.kafkaConsumerStartedPollingEventPublisher = startedEventPublisher;
        this.kafkaConsumerSubscribedEventPublisher = subscribedEventPublisher;
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
    public void process(BeanDefinition<?> beanDefinition, ExecutableMethod<?, ?> method) {
        List<AnnotationValue<Topic>> topicAnnotations = method.getDeclaredAnnotationValuesByType(Topic.class);
        final AnnotationValue<KafkaListener> consumerAnnotation = method.getAnnotation(KafkaListener.class);
        if (CollectionUtils.isEmpty(topicAnnotations)) {
            topicAnnotations = beanDefinition.getDeclaredAnnotationValuesByType(Topic.class);
        }
        if (consumerAnnotation == null || CollectionUtils.isEmpty(topicAnnotations)) {
            return; // No topics to consume
        }
        final Class<?> beanType = beanDefinition.getBeanType();
        String groupId = consumerAnnotation.stringValue("groupId")
                .filter(StringUtils::isNotEmpty)
                .orElseGet(() -> applicationConfiguration.getName().orElse(beanType.getName()));
        final String clientId = consumerAnnotation.stringValue("clientId")
                .filter(StringUtils::isNotEmpty)
                .orElseGet(() -> applicationConfiguration.getName().map(s -> s + '-' + NameUtils.hyphenate(beanType.getSimpleName())).orElse(null));
        final OffsetStrategy offsetStrategy = consumerAnnotation.enumValue("offsetStrategy", OffsetStrategy.class)
                .orElse(OffsetStrategy.AUTO);
        final AbstractKafkaConsumerConfiguration<?, ?> consumerConfigurationDefaults = beanContext.findBean(AbstractKafkaConsumerConfiguration.class, Qualifiers.byName(groupId))
                .orElse(defaultConsumerConfiguration);
        if (consumerAnnotation.isTrue("uniqueGroupId")) {
            groupId = groupId + "_" + UUID.randomUUID();
        }
        final DefaultKafkaConsumerConfiguration<?, ?> consumerConfiguration = new DefaultKafkaConsumerConfiguration<>(consumerConfigurationDefaults);
        final Properties properties = createConsumerProperties(consumerAnnotation, consumerConfiguration, clientId, groupId, offsetStrategy);
        configureDeserializers(method, consumerConfiguration);
        submitConsumerThreads(method, clientId, groupId, offsetStrategy, topicAnnotations, consumerAnnotation, consumerConfiguration, properties, beanType);
    }

    @Override
    @PreDestroy
    public void close() {
        for (ConsumerState consumerState : consumers.values()) {
            consumerState.kafkaConsumer.wakeup();
        }
        for (ConsumerState consumerState : consumers.values()) {
            if (consumerState.isPolling()) {
                final Instant start = Instant.now();
                Instant silentTime = start;
                do {
                    if (LOG.isTraceEnabled()) {
                        final Instant now = Instant.now();
                        if (now.isAfter(silentTime)) {
                            LOG.trace("Consumer {} is not closed yet (waiting {})", consumerState.info.clientId, Duration.between(start, now));
                            // Inhibit TRACE messages for a while to avoid polluting the logs
                            silentTime = now.plusSeconds(5);
                        }
                    }
                } while (consumerState.isPolling());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Consumer {} is closed", consumerState.info.clientId);
            }
        }
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

    void scheduleTask(Duration delay, Runnable command) {
        taskScheduler.schedule(delay, command);
    }

    <K, V> Producer<K, V> getProducer(String id, Class<K> keyType, Class<V> valueType) {
        return producerRegistry.getProducer(id, Argument.of(keyType), Argument.of(valueType));
    }

    <K, V> Producer<K, V> getTransactionalProducer(@Nullable String clientId, @Nullable String transactionalId, Class<K> keyClass, Class<V> valueClass) {
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

    <T, R> BoundExecutable<T, R> bind(ExecutableMethod<T, R> method, Map<Argument<?>, Object> boundArguments, ConsumerRecord<?, ?> consumerRecord) {
        final ExecutableBinder<ConsumerRecord<?, ?>> executableBinder = new DefaultExecutableBinder<>(boundArguments);
        return executableBinder.bind(method, binderRegistry, consumerRecord);
    }

    <T, R> BoundExecutable<T, R> bindAsBatch(ExecutableMethod<T, R> method, Map<Argument<?>, Object> boundArguments, ConsumerRecords<?, ?> consumerRecords) {
        final ExecutableBinder<ConsumerRecords<?, ?>> batchBinder = new DefaultExecutableBinder<>(boundArguments);
        return batchBinder.bind(method, batchBinderRegistry, consumerRecords);
    }

    @SuppressWarnings("rawtypes")
    private Properties createConsumerProperties(final AnnotationValue<KafkaListener> consumerAnnotation,
                                                final DefaultKafkaConsumerConfiguration consumerConfiguration,
                                                final String clientId,
                                                final String groupId,
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

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        if (clientId != null) {
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }

        properties.putAll(consumerAnnotation.getProperties("properties", "name"));
        return properties;
    }

    private void debugDeserializationConfiguration(final ExecutableMethod<?, ?> method, final DefaultKafkaConsumerConfiguration<?, ?> consumerConfiguration,
                                                   final Properties properties) {
        if (!LOG.isDebugEnabled()) {
            return;
        }
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
    private void submitConsumerThreads(final ExecutableMethod<?, ?> method,
                                       final String clientId,
                                       final String groupId,
                                       final OffsetStrategy offsetStrategy,
                                       final List<AnnotationValue<Topic>> topicAnnotations,
                                       final AnnotationValue<KafkaListener> consumerAnnotation,
                                       final DefaultKafkaConsumerConfiguration<?, ?> consumerConfiguration,
                                       final Properties properties,
                                       final Class<?> beanType) {
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
            setupConsumerSubscription(method, topicAnnotations, consumerBean, kafkaConsumer);
            kafkaConsumerSubscribedEventPublisher.publishEvent(new KafkaConsumerSubscribedEvent(kafkaConsumer));
            final ConsumerInfo consumerInfo = new ConsumerInfo(finalClientId, groupId, offsetStrategy, consumerAnnotation, method);
            final ConsumerState consumerState = new ConsumerState(this, consumerInfo, kafkaConsumer, consumerBean);
            consumers.put(finalClientId, consumerState);
            executorService.submit(consumerState::threadPollLoop);
        }
    }

    private static void setupConsumerSubscription(final ExecutableMethod<?, ?> method, final List<AnnotationValue<Topic>> topicAnnotations,
                                                  final Object consumerBean, final Consumer<?, ?> kafkaConsumer) {
        for (final AnnotationValue<Topic> topicAnnotation : topicAnnotations) {

            final String[] topicNames = topicAnnotation.stringValues();
            final String[] patterns = topicAnnotation.stringValues("patterns");
            final boolean hasTopics = ArrayUtils.isNotEmpty(topicNames);
            final boolean hasPatterns = ArrayUtils.isNotEmpty(patterns);

            if (!hasTopics && !hasPatterns) {
                throw new MessagingSystemException("Either a topic or a topic must be specified for method: " + method);
            }

            if (hasTopics) {
                final List<String> topics = Arrays.asList(topicNames);
                if (consumerBean instanceof ConsumerSeekAware csa) {
                    kafkaConsumer.subscribe(topics, new ConsumerSeekAwareAdapter(KafkaSeeker.newInstance(kafkaConsumer), csa));
                } else if (consumerBean instanceof ConsumerRebalanceListener crl) {
                    kafkaConsumer.subscribe(topics, crl);
                } else {
                    kafkaConsumer.subscribe(topics);
                }

                if (LOG.isInfoEnabled()) {
                    LOG.info("Kafka listener [{}] subscribed to topics: {}", logMethod(method), topics);
                }
            }

            if (hasPatterns) {
                for (final String pattern : patterns) {
                    final Pattern compiledPattern;
                    try {
                        compiledPattern = Pattern.compile(pattern);
                    } catch (Exception e) {
                        throw new MessagingSystemException("Invalid topic pattern [" + pattern + "] for method [" + method + "]: " + e.getMessage(), e);
                    }
                    if (consumerBean instanceof ConsumerSeekAware csa) {
                        kafkaConsumer.subscribe(compiledPattern, new ConsumerSeekAwareAdapter(KafkaSeeker.newInstance(kafkaConsumer), csa));
                    } else if (consumerBean instanceof ConsumerRebalanceListener crl) {
                        kafkaConsumer.subscribe(compiledPattern, crl);
                    } else {
                        kafkaConsumer.subscribe(compiledPattern);
                    }
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Kafka listener [{}] subscribed to topics pattern: {}", logMethod(method), pattern);
                    }
                }
            }
        }
    }

    private static Argument<?> findBodyArgument(ExecutableMethod<?, ?> method) {
        return Arrays.stream(method.getArguments())
                .filter(arg -> arg.getType() == ConsumerRecord.class || arg.getAnnotationMetadata().hasAnnotation(MessageBody.class))
                .findFirst()
                .orElseGet(() -> Arrays.stream(method.getArguments())
                        .filter(arg -> !arg.getAnnotationMetadata().hasStereotype(Bindable.class))
                        .findFirst()
                        .orElse(null));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void configureDeserializers(final ExecutableMethod<?, ?> method, final DefaultKafkaConsumerConfiguration consumerConfiguration) {
        final Properties properties = consumerConfiguration.getConfig();
        // figure out the Key deserializer
        boolean batch = method.isTrue(KafkaListener.class, "batch");

        Argument<?> tempBodyArg = findBodyArgument(method);

        final Argument<?> bodyArgument = batch && tempBodyArg != null ? getComponentType(tempBodyArg) : tempBodyArg;

        if (!properties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) && consumerConfiguration.getKeyDeserializer().isEmpty()) {
            final Optional<Argument<?>> keyArgument = Arrays.stream(method.getArguments())
                    .filter(arg -> arg.isAnnotationPresent(KafkaKey.class))
                    .findFirst();
            if (keyArgument.isPresent()) {
                consumerConfiguration.setKeyDeserializer(serdeRegistry.pickDeserializer(keyArgument.get()));
            } else {
                //noinspection SingleStatementInBlock
                if (bodyArgument != null && ConsumerRecord.class.isAssignableFrom(bodyArgument.getType())) {
                    final Optional<Argument<?>> keyType = bodyArgument.getTypeVariable("K");
                    if (keyType.isPresent()) {
                        consumerConfiguration.setKeyDeserializer(serdeRegistry.pickDeserializer(keyType.get()));
                    } else {
                        consumerConfiguration.setKeyDeserializer(new ByteArrayDeserializer());
                    }
                } else {
                    consumerConfiguration.setKeyDeserializer(new ByteArrayDeserializer());
                }
            }
        }

        // figure out the Value deserializer
        if (!properties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) && consumerConfiguration.getValueDeserializer().isEmpty()) {
            if (bodyArgument == null) {
                //noinspection SingleStatementInBlock
                consumerConfiguration.setValueDeserializer(new StringDeserializer());
            } else {
                if (ConsumerRecord.class.isAssignableFrom(bodyArgument.getType())) {
                    final Optional<Argument<?>> valueType = bodyArgument.getTypeVariable("V");
                    if (valueType.isPresent()) {
                        consumerConfiguration.setValueDeserializer(serdeRegistry.pickDeserializer(valueType.get()));
                    } else {
                        consumerConfiguration.setValueDeserializer(new StringDeserializer());
                    }
                } else {
                    consumerConfiguration.setValueDeserializer(serdeRegistry.pickDeserializer(bodyArgument));
                }
            }
        }
        debugDeserializationConfiguration(method, consumerConfiguration, properties);
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
