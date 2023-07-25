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
import io.micronaut.configuration.kafka.KafkaAcknowledgement;
import io.micronaut.configuration.kafka.KafkaMessage;
import io.micronaut.configuration.kafka.ProducerRegistry;
import io.micronaut.configuration.kafka.TransactionalProducerRegistry;
import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
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
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler;
import io.micronaut.configuration.kafka.serde.SerdeRegistry;
import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Blocking;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.bind.BoundExecutable;
import io.micronaut.core.bind.DefaultExecutableBinder;
import io.micronaut.core.bind.ExecutableBinder;
import io.micronaut.core.bind.annotation.Bindable;
import io.micronaut.core.naming.NameUtils;
import io.micronaut.core.reflect.ReflectionUtils;
import io.micronaut.core.type.Argument;
import io.micronaut.core.type.ReturnType;
import io.micronaut.core.util.ArgumentUtils;
import io.micronaut.core.util.ArrayUtils;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.messaging.Acknowledgement;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.messaging.annotation.SendTo;
import io.micronaut.messaging.exceptions.MessagingSystemException;
import io.micronaut.runtime.ApplicationConfiguration;
import io.micronaut.scheduling.ScheduledExecutorTaskScheduler;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.TaskScheduler;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import java.util.function.Function;
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
     */
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
            TransactionalProducerRegistry transactionalProducerRegistry) {
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
    public <K, V> Consumer<K, V> getConsumer(@NonNull String id) {
        ArgumentUtils.requireNonNull("id", id);
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
            while (consumerState.closedState == ConsumerCloseState.POLLING) {
                LOG.trace("consumer not closed yet");
            }
        }
        consumers.clear();
    }

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
        final Optional keyDeserializer = consumerConfiguration.getKeyDeserializer();
        if (consumerConfiguration.getKeyDeserializer().isPresent()) {
            LOG.debug("Using key deserializer [{}] for Kafka listener: {}", keyDeserializer.get(), logMethod(method));
        } else {
            LOG.debug("Using key deserializer [{}] for Kafka listener: {}", properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), logMethod(method));
        }
        final Optional valueDeserializer = consumerConfiguration.getValueDeserializer();
        if (valueDeserializer.isPresent()) {
            LOG.debug("Using value deserializer [{}] for Kafka listener: {}", valueDeserializer.get(), logMethod(method));
        } else {
            LOG.debug("Using value deserializer [{}] for Kafka listener: {}", properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), logMethod(method));
        }
    }

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
                if (consumerThreads > 1) {
                    finalClientId = clientId + '-' + clientIdGenerator.incrementAndGet();
                } else {
                    finalClientId = clientId;
                }
                properties.put(ConsumerConfig.CLIENT_ID_CONFIG, finalClientId);
            } else {
                finalClientId = "kafka-consumer-" + clientIdGenerator.incrementAndGet();
            }
            submitConsumerThread(method, finalClientId, groupId, offsetStrategy, topicAnnotations, consumerAnnotation, consumerConfiguration, beanType);
        }
    }

    private void submitConsumerThread(final ExecutableMethod<?, ?> method,
                                      final String clientId,
                                      final String groupId,
                                      final OffsetStrategy offsetStrategy,
                                      final List<AnnotationValue<Topic>> topicAnnotations,
                                      final AnnotationValue<KafkaListener> consumerAnnotation,
                                      final DefaultKafkaConsumerConfiguration<?, ?> consumerConfiguration,
                                      final Class<?> beanType) {
        final Consumer<?, ?> kafkaConsumer = beanContext.createBean(Consumer.class, consumerConfiguration);
        final Object consumerBean = beanContext.getBean(beanType);
        if (consumerBean instanceof ConsumerAware ca) {
            //noinspection unchecked
            ca.setKafkaConsumer(kafkaConsumer);
        }
        setupConsumerSubscription(method, topicAnnotations, consumerBean, kafkaConsumer);
        ConsumerState consumerState = new ConsumerState(clientId, groupId, offsetStrategy, kafkaConsumer, consumerBean, Collections.unmodifiableSet(kafkaConsumer.subscription()), consumerAnnotation, method);
        consumers.put(clientId, consumerState);
        executorService.submit(() -> createConsumerThreadPollLoop(method, consumerState));
    }

    private void createConsumerThreadPollLoop(final ExecutableMethod<?, ?> method,
                                              final ConsumerState consumerState) {

        final boolean isBatch = method.isTrue(KafkaListener.class, "batch");
        final Duration pollTimeout = method.getValue(KafkaListener.class, "pollTimeout", Duration.class)
                .orElseGet(() -> Duration.ofMillis(100));
        final Optional<Argument<?>> consumerArg = Arrays.stream(method.getArguments())
                .filter(arg -> Consumer.class.isAssignableFrom(arg.getType()))
                .findFirst();
        final Optional<Argument<?>> ackArg = Arrays.stream(method.getArguments())
                .filter(arg -> Acknowledgement.class.isAssignableFrom(arg.getType()))
                .findFirst();

        try (Consumer<?, ?> kafkaConsumer = consumerState.kafkaConsumer) {

            final boolean trackPartitions = ackArg.isPresent() || consumerState.offsetStrategy == OffsetStrategy.SYNC_PER_RECORD || consumerState.offsetStrategy == OffsetStrategy.ASYNC_PER_RECORD;
            final Map<Argument<?>, Object> boundArguments = new HashMap<>(2);
            consumerArg.ifPresent(argument -> boundArguments.put(argument, kafkaConsumer));

            //noinspection InfiniteLoopStatement
            while (true) {
                final Set<TopicPartition> newAssignments = Collections.unmodifiableSet(kafkaConsumer.assignment());
                if (LOG.isInfoEnabled() && !newAssignments.equals(consumerState.assignments)) {
                    LOG.info("Consumer [{}] assignments changed: {} -> {}", consumerState.clientId, consumerState.assignments, newAssignments);
                }
                consumerState.assignments = newAssignments;
                if (consumerState.autoPaused) {
                    consumerState.pause(consumerState.assignments);
                    kafkaConsumer.pause(consumerState.assignments);
                }
                boolean failed = true;
                try {
                    consumerState.pauseTopicPartitions();
                    final ConsumerRecords<?, ?> consumerRecords = kafkaConsumer.poll(pollTimeout);
                    consumerState.closedState = ConsumerCloseState.POLLING;
                    failed = true;
                    consumerState.resumeTopicPartitions();

                    if (consumerRecords == null || consumerRecords.count() <= 0) {
                        continue; // No consumer records to process
                    }

                    if (isBatch) {
                        failed = !processConsumerRecordsAsBatch(consumerState, method, boundArguments, consumerRecords);
                    } else {
                        failed = !processConsumerRecords(consumerState, method, boundArguments, trackPartitions, ackArg, consumerRecords);
                    }
                    if (!failed) {
                        if (consumerState.offsetStrategy == OffsetStrategy.SYNC) {
                            try {
                                kafkaConsumer.commitSync();
                            } catch (CommitFailedException e) {
                                handleException(consumerState, null, e);
                            }
                        } else if (consumerState.offsetStrategy == OffsetStrategy.ASYNC) {
                            kafkaConsumer.commitAsync(resolveCommitCallback(consumerState.consumerBean));
                        }
                    }

                } catch (WakeupException e) {
                    try {
                        if (!failed && consumerState.offsetStrategy != OffsetStrategy.DISABLED) {
                            kafkaConsumer.commitSync();
                        }
                    } catch (Throwable ex) {
                        LOG.warn("Error committing Kafka offsets on shutdown: {}", ex.getMessage(), ex);
                    }
                    throw e;
                } catch (Throwable e) {
                    handleException(consumerState, null, e);
                }
            }
        } catch (WakeupException e) {
            consumerState.closedState = ConsumerCloseState.CLOSED;
        }
    }

    private boolean processConsumerRecords(final ConsumerState consumerState,
                                           final ExecutableMethod<?, ?> method,
                                           final Map<Argument<?>, Object> boundArguments,
                                           final boolean trackPartitions,
                                           final Optional<Argument<?>> ackArg,
                                           final ConsumerRecords<?, ?> consumerRecords) {
        final ExecutableBinder<ConsumerRecord<?, ?>> executableBinder = new DefaultExecutableBinder<>(boundArguments);
        final Map<TopicPartition, OffsetAndMetadata> currentOffsets = trackPartitions ? new HashMap<>() : null;

        Iterator<? extends ConsumerRecord<?, ?>> iterator = consumerRecords.iterator();
        while (iterator.hasNext()) {
            ConsumerRecord<?, ?> consumerRecord = iterator.next();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Kafka consumer [{}] received record: {}", logMethod(method), consumerRecord);
            }

            if (trackPartitions) {
                final TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset() + 1, null);
                currentOffsets.put(topicPartition, offsetAndMetadata);
            }

            Consumer<?, ?> kafkaConsumer = consumerState.kafkaConsumer;
            ackArg.ifPresent(argument -> boundArguments.put(argument, (KafkaAcknowledgement) () -> kafkaConsumer.commitSync(currentOffsets)));

            try {
                final BoundExecutable boundExecutable = executableBinder.bind(method, binderRegistry, consumerRecord);
                final Object result = boundExecutable.invoke(consumerState.consumerBean);
                if (result != null) {
                    final Flux<?> resultFlowable;
                    final boolean isBlocking;
                    if (Publishers.isConvertibleToPublisher(result)) {
                        resultFlowable = Flux.from(Publishers.convertPublisher(beanContext.getConversionService(), result, Publisher.class));
                        isBlocking = method.hasAnnotation(Blocking.class);
                    } else {
                        resultFlowable = Flux.just(result);
                        isBlocking = true;
                    }
                    handleResultFlux(consumerState, method, consumerRecord, resultFlowable, isBlocking, consumerRecords);
                }
            } catch (Throwable e) {
                if (resolveWithErrorStrategy(consumerState, consumerRecord, e)) {
                    resetTheFollowingPartitions(consumerRecord, consumerState, iterator);
                    return false;
                }
            }

            if (consumerState.offsetStrategy == OffsetStrategy.SYNC_PER_RECORD) {
                try {
                    kafkaConsumer.commitSync(currentOffsets);
                } catch (CommitFailedException e) {
                    handleException(consumerState, consumerRecord, e);
                }
            } else if (consumerState.offsetStrategy == OffsetStrategy.ASYNC_PER_RECORD) {
                kafkaConsumer.commitAsync(currentOffsets, resolveCommitCallback(consumerState.consumerBean));
            }
        }
        return true;
    }

    private void resetTheFollowingPartitions(ConsumerRecord<?, ?> errorConsumerRecord, ConsumerState consumerState, Iterator<? extends ConsumerRecord<?, ?>> iterator) {
        Set<Integer> processedPartition = new HashSet<>();
        processedPartition.add(errorConsumerRecord.partition());
        while (iterator.hasNext()) {
            ConsumerRecord<?, ?> consumerRecord = iterator.next();
            if (!processedPartition.add(consumerRecord.partition())) {
                continue;
            }
            TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
            consumerState.kafkaConsumer.seek(topicPartition, consumerRecord.offset());
        }
    }

    private boolean resolveWithErrorStrategy(ConsumerState consumerState,
                                             ConsumerRecord<?, ?> consumerRecord,
                                             Throwable e) {

        ErrorStrategyValue currentErrorStrategy = consumerState.errorStrategy;

        if (isRetryErrorStrategy(currentErrorStrategy) && consumerState.errorStrategyExceptions.length > 0 && Arrays.stream(consumerState.errorStrategyExceptions).noneMatch(error -> error.equals(e.getClass()))) {
            if (consumerState.partitionRetries != null) {
                consumerState.partitionRetries.remove(consumerRecord.partition());
            }
            // Skip the failing record
            currentErrorStrategy = ErrorStrategyValue.RESUME_AT_NEXT_RECORD;
        }

        if (isRetryErrorStrategy(currentErrorStrategy) && consumerState.errorStrategyRetryCount != 0) {
            if (consumerState.partitionRetries == null) {
                consumerState.partitionRetries = new HashMap<>();
            }
            int partition = consumerRecord.partition();
            PartitionRetryState retryState = consumerState.partitionRetries.computeIfAbsent(partition, t -> new PartitionRetryState());
            if (retryState.currentRetryOffset != consumerRecord.offset()) {
                retryState.currentRetryOffset = consumerRecord.offset();
                retryState.currentRetryCount = 1;
            } else {
                retryState.currentRetryCount++;
            }

            if (consumerState.errorStrategyRetryCount >= retryState.currentRetryCount) {
                TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), partition);
                consumerState.kafkaConsumer.seek(topicPartition, consumerRecord.offset());

                Duration retryDelay = computeRetryDelay(currentErrorStrategy, consumerState.errorStrategyRetryDelay, retryState.currentRetryCount);
                if (retryDelay != null) {
                    // in the stop on error strategy, pause the consumer and resume after the retryDelay duration
                    Set<TopicPartition> paused = Collections.singleton(topicPartition);
                    consumerState.pause(paused);
                    taskScheduler.schedule(retryDelay, () -> consumerState.resume(paused));
                }
                // skip handle exception
                return true;
            } else {
                consumerState.partitionRetries.remove(partition);
                // Skip the failing record
                currentErrorStrategy = ErrorStrategyValue.RESUME_AT_NEXT_RECORD;
            }
        }

        handleException(consumerState, consumerRecord, e);

        return currentErrorStrategy != ErrorStrategyValue.RESUME_AT_NEXT_RECORD;
    }

    private static boolean isRetryErrorStrategy(ErrorStrategyValue currentErrorStrategy) {
        return currentErrorStrategy == ErrorStrategyValue.RETRY_ON_ERROR || currentErrorStrategy == ErrorStrategyValue.RETRY_EXPONENTIALLY_ON_ERROR;
    }

    private Duration computeRetryDelay(ErrorStrategyValue errorStrategy, Duration fixedRetryDelay, long retryAttempts) {
        if (errorStrategy == ErrorStrategyValue.RETRY_ON_ERROR) {
            return fixedRetryDelay;
        } else if (errorStrategy == ErrorStrategyValue.RETRY_EXPONENTIALLY_ON_ERROR) {
            return fixedRetryDelay.multipliedBy(1L << (retryAttempts - 1));
        } else {
            return Duration.ZERO;
        }
    }

    private boolean processConsumerRecordsAsBatch(final ConsumerState consumerState,
                                                  final ExecutableMethod<?, ?> method,
                                                  final Map<Argument<?>, Object> boundArguments,
                                                  final ConsumerRecords<?, ?> consumerRecords) {
        final ExecutableBinder<ConsumerRecords<?, ?>> batchBinder = new DefaultExecutableBinder<>(boundArguments);
        final BoundExecutable boundExecutable = batchBinder.bind(method, batchBinderRegistry, consumerRecords);
        Object result = boundExecutable.invoke(consumerState.consumerBean);

        if (result != null) {
            if (result.getClass().isArray()) {
                result = Arrays.asList((Object[]) result);
            }

            final boolean isPublisher = Publishers.isConvertibleToPublisher(result);
            final Flux<?> resultFlux;
            if (result instanceof Iterable iterable) {
                resultFlux = Flux.fromIterable(iterable);
            } else if (isPublisher) {
                resultFlux = Flux.from(Publishers.convertPublisher(beanContext.getConversionService(), result, Publisher.class));
            } else {
                resultFlux = Flux.just(result);
            }

            final Iterator<? extends ConsumerRecord<?, ?>> iterator = consumerRecords.iterator();
            final boolean isBlocking = !isPublisher || method.hasAnnotation(Blocking.class);
            if (isBlocking) {
                List<?> objects = resultFlux.collectList().block();
                for (Object object : objects) {
                    if (iterator.hasNext()) {
                        final ConsumerRecord<?, ?> consumerRecord = iterator.next();
                        handleResultFlux(consumerState, method, consumerRecord, Flux.just(object), isBlocking, consumerRecords);
                    }
                }
            } else {
                resultFlux.subscribe(o -> {
                    if (iterator.hasNext()) {
                        final ConsumerRecord<?, ?> consumerRecord = iterator.next();
                        handleResultFlux(consumerState, method, consumerRecord, Flux.just(o), isBlocking, consumerRecords);
                    }
                });
            }
        }
        return true;
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
                if (consumerBean instanceof ConsumerRebalanceListener crl) {
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
                    if (consumerBean instanceof ConsumerRebalanceListener crl) {
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

    private void handleException(final ConsumerState consumerState,
                                 final ConsumerRecord<?, ?> consumerRecord,
                                 final Throwable e) {
        handleException(consumerState.consumerBean, new KafkaListenerException(e, consumerState.consumerBean, consumerState.kafkaConsumer, consumerRecord));
    }

    private void handleException(final Object consumerBean, final KafkaListenerException kafkaListenerException) {
        if (consumerBean instanceof KafkaListenerExceptionHandler kle) {
            kle.handle(kafkaListenerException);
        } else {
            exceptionHandler.handle(kafkaListenerException);
        }
    }

    @SuppressWarnings({"SubscriberImplementation", "unchecked"})
    private void handleResultFlux(ConsumerState consumerState,
                                  ExecutableMethod<?, ?> method,
                                  ConsumerRecord<?, ?> consumerRecord,
                                  Flux<?> resultFlowable,
                                  boolean isBlocking,
                                  ConsumerRecords<?, ?> consumerRecords) {

        Flux<RecordMetadata> recordMetadataProducer = resultFlowable
                .flatMap((Function<Object, Publisher<RecordMetadata>>) value -> {
                    if (consumerState.sendToDestinationTopics != null) {
                        Object key = consumerRecord.key();
                        if (value != null) {
                            Producer kafkaProducer;
                            if (consumerState.useSendOffsetsToTransaction) {
                                kafkaProducer = transactionalProducerRegistry.getTransactionalProducer(
                                        consumerState.producerClientId,
                                        consumerState.producerTransactionalId,
                                        Argument.of(byte[].class),
                                        Argument.of(Object.class)
                                );
                            } else {
                                kafkaProducer = producerRegistry.getProducer(
                                        consumerState.producerClientId == null ? consumerState.groupId : consumerState.producerClientId,
                                        Argument.of((Class) (key != null ? key.getClass() : byte[].class)),
                                        Argument.of(value.getClass())
                                );
                            }
                            return Flux.create(emitter -> {
                                try {
                                    if (consumerState.useSendOffsetsToTransaction) {
                                        try {
                                            LOG.trace("Beginning transaction for producer: {}", consumerState.producerTransactionalId);
                                            kafkaProducer.beginTransaction();
                                        } catch (ProducerFencedException e) {
                                            handleProducerFencedException(kafkaProducer, e);
                                        }
                                    }
                                    for (String destinationTopic : consumerState.sendToDestinationTopics) {
                                        if (consumerState.isMessagesIterableReturnType) {
                                            Iterable<KafkaMessage> messages = (Iterable<KafkaMessage>) value;
                                            for (KafkaMessage message : messages) {
                                                ProducerRecord record = createFromMessage(destinationTopic, message);
                                                kafkaProducer.send(record, (metadata, exception) -> {
                                                    if (exception != null) {
                                                        emitter.error(exception);
                                                    } else {
                                                        emitter.next(metadata);
                                                    }
                                                });
                                            }
                                        } else {
                                            ProducerRecord record;
                                            if (consumerState.isMessageReturnType) {
                                                record = createFromMessage(destinationTopic, (KafkaMessage) value);
                                            } else {
                                                record = new ProducerRecord(destinationTopic, null, key, value, consumerRecord.headers());
                                            }
                                            LOG.trace("Sending record: {} for producer: {} {}", record, kafkaProducer, consumerState.producerTransactionalId);
                                            kafkaProducer.send(record, (metadata, exception) -> {
                                                if (exception != null) {
                                                    emitter.error(exception);
                                                } else {
                                                    emitter.next(metadata);
                                                }
                                            });
                                        }
                                    }
                                    if (consumerState.useSendOffsetsToTransaction) {
                                        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                                        for (TopicPartition partition : consumerRecords.partitions()) {
                                            List<? extends ConsumerRecord<?, ?>> partitionedRecords = consumerRecords.records(partition);
                                            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
                                            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
                                        }
                                        try {
                                            LOG.trace("Sending offsets: {} to transaction for producer: {} and customer group id: {}", offsetsToCommit, consumerState.producerTransactionalId, consumerState.groupId);
                                            kafkaProducer.sendOffsetsToTransaction(offsetsToCommit, new ConsumerGroupMetadata(consumerState.groupId));
                                            LOG.trace("Committing transaction for producer: {}", consumerState.producerTransactionalId);
                                            kafkaProducer.commitTransaction();
                                            LOG.trace("Committed transaction for producer: {}", consumerState.producerTransactionalId);
                                        } catch (ProducerFencedException e) {
                                            handleProducerFencedException(kafkaProducer, e);
                                        }
                                    }
                                    emitter.complete();
                                } catch (Exception e) {
                                    if (consumerState.useSendOffsetsToTransaction) {
                                        try {
                                            LOG.trace("Aborting transaction for producer: {} because of error: {}", consumerState.producerTransactionalId, e.getMessage());
                                            kafkaProducer.abortTransaction();
                                        } catch (ProducerFencedException ex) {
                                            handleProducerFencedException(kafkaProducer, ex);
                                        }
                                    }
                                    emitter.error(e);
                                }
                            });
                        }
                        return Flux.empty();
                    }
                    return Flux.empty();
                });

        recordMetadataProducer = recordMetadataProducer.onErrorResume((Function<Throwable, Publisher<RecordMetadata>>) throwable -> {
            handleException(consumerState.consumerBean, new KafkaListenerException(
                    "Error occurred processing record [" + consumerRecord + "] with Kafka reactive consumer [" + method + "]: " + throwable.getMessage(),
                    throwable,
                    consumerState.consumerBean,
                    consumerState.kafkaConsumer,
                    consumerRecord
            ));

            if (consumerState.redelivery) {
                LOG.debug("Attempting redelivery of record [{}] following error", consumerRecord);

                Object key = consumerRecord.key();
                Object value = consumerRecord.value();

                if (key != null && value != null) {
                    Producer kafkaProducer = producerRegistry.getProducer(
                            consumerState.producerClientId == null ? consumerState.groupId : consumerState.producerClientId,
                            Argument.of(key.getClass()),
                            Argument.of(value.getClass())
                    );

                    ProducerRecord record = new ProducerRecord(
                            consumerRecord.topic(),
                            consumerRecord.partition(),
                            key,
                            value,
                            consumerRecord.headers()
                    );

                    return producerSend(consumerState, kafkaProducer, record).doOnError(ex -> {
                        handleException(consumerState.consumerBean, new KafkaListenerException(
                                "Redelivery failed for record [" + consumerRecord + "] with Kafka reactive consumer [" + method + "]: " + throwable.getMessage(),
                                throwable,
                                consumerState.consumerBean,
                                consumerState.kafkaConsumer,
                                consumerRecord
                        ));
                    });
                }
            }
            return Flux.empty();
        });

        if (isBlocking) {
            List<RecordMetadata> listRecords = recordMetadataProducer.collectList().block();
            LOG.trace("Method [{}] produced record metadata: {}", method, listRecords);
        } else {
            recordMetadataProducer.subscribe(recordMetadata -> LOG.trace("Method [{}] produced record metadata: {}", logMethod(method), recordMetadata));
        }
    }

    private Mono<RecordMetadata> producerSend(ConsumerState consumerState, Producer<?, ?> producer, ProducerRecord record) {
        LOG.trace("Sending record: {} for producer: {} {}", record, producer, consumerState.producerTransactionalId);
        return Mono.create(emitter -> producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                emitter.error(exception);
            } else {
                emitter.success(metadata);
            }
        }));
    }

    private ProducerRecord createFromMessage(String topic, KafkaMessage<?, ?> message) {
        return new ProducerRecord(
                message.getTopic() == null ? topic : message.getTopic(),
                message.getPartition(),
                message.getTimestamp(),
                message.getKey(),
                message.getBody(),
                convertHeaders(message));
    }

    private List<RecordHeader> convertHeaders(KafkaMessage<?, ?> message) {
        return message.getHeaders() == null ? null : message.getHeaders().entrySet()
                .stream()
                .map(e -> new RecordHeader(e.getKey(), e.getValue().toString().getBytes(StandardCharsets.UTF_8))).toList();
    }

    private void handleProducerFencedException(Producer<?, ?> producer, ProducerFencedException e) {
        LOG.error("Failed accessing the producer: " + producer, e);
        transactionalProducerRegistry.close(producer);
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

    private void configureDeserializers(final ExecutableMethod<?, ?> method, final DefaultKafkaConsumerConfiguration consumerConfiguration) {
        final Properties properties = consumerConfiguration.getConfig();
        // figure out the Key deserializer
        boolean batch = method.isTrue(KafkaListener.class, "batch");

        Argument<?> tempBodyArg = findBodyArgument(method);

        final Argument<?> bodyArgument = batch && tempBodyArg != null ? getComponentType(tempBodyArg) : tempBodyArg;

        if (!properties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) && !consumerConfiguration.getKeyDeserializer().isPresent()) {
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
        if (!properties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) && !consumerConfiguration.getValueDeserializer().isPresent()) {
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

    private static Argument getComponentType(final Argument<?> argument) {
        final Class<?> argumentType = argument.getType();
        return argumentType.isArray()
                ? Argument.of(argumentType.getComponentType())
                : argument.getFirstTypeVariable().orElse(argument);
    }

    private static OffsetCommitCallback resolveCommitCallback(final Object consumerBean) {
        return (offsets, exception) -> {
            if (consumerBean instanceof OffsetCommitCallback occ) {
                occ.onComplete(offsets, exception);
            } else if (exception != null) {
                LOG.error("Error asynchronously committing Kafka offsets [{}]: {}", offsets, exception.getMessage(), exception);
            }
        };
    }

    private static String logMethod(ExecutableMethod<?, ?> method) {
        return method.getDeclaringType().getSimpleName() + "#" + method.getName();
    }

    /**
     * The internal state of the consumer.
     *
     * @author Denis Stepanov
     */
    private static final class ConsumerState {
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
        ConsumerCloseState closedState;

        private ConsumerState(String clientId, String groupId, OffsetStrategy offsetStrategy, Consumer<?, ?> consumer, Object consumerBean, Set<String> subscriptions,
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

            if (isRetryErrorStrategy(errorStrategy)) {
                Duration retryDelay = errorStrategyAnnotation.get("retryDelay", Duration.class)
                        .orElse(Duration.ofSeconds(ErrorStrategy.DEFAULT_DELAY_IN_SECONDS));
                this.errorStrategyRetryDelay = retryDelay.isNegative() || retryDelay.isZero() ? null : retryDelay;
                this.errorStrategyRetryCount = errorStrategyAnnotation.intValue("retryCount").orElse(ErrorStrategy.DEFAULT_RETRY_COUNT);
                //noinspection unchecked
                this.errorStrategyExceptions = (Class<? extends Throwable>[]) errorStrategyAnnotation.classValues("exceptionTypes");
            } else {
                this.errorStrategyRetryDelay = null;
                this.errorStrategyRetryCount = 0;
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

    /**
     * Topic retry status.
     */
    private static final class PartitionRetryState {
        long currentRetryOffset;
        int currentRetryCount;
    }

    private enum ConsumerCloseState {
        NOT_STARTED, POLLING, CLOSED
    }

}
