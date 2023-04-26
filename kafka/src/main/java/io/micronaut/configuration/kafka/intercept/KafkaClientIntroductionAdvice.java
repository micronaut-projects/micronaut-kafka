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
package io.micronaut.configuration.kafka.intercept;

import io.micronaut.aop.InterceptedMethod;
import io.micronaut.aop.InterceptorBean;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaPartition;
import io.micronaut.configuration.kafka.annotation.KafkaPartitionKey;
import io.micronaut.configuration.kafka.annotation.KafkaTimestamp;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.config.AbstractKafkaProducerConfiguration;
import io.micronaut.configuration.kafka.config.DefaultKafkaProducerConfiguration;
import io.micronaut.configuration.kafka.config.KafkaProducerConfiguration;
import io.micronaut.configuration.kafka.serde.SerdeRegistry;
import io.micronaut.context.BeanContext;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.bind.annotation.Bindable;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.core.type.ReturnType;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.messaging.annotation.MessageHeader;
import io.micronaut.messaging.exceptions.MessagingClientException;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Implementation of the {@link io.micronaut.configuration.kafka.annotation.KafkaClient} advice annotation.
 *
 * @author Graeme Rocher
 * @see io.micronaut.configuration.kafka.annotation.KafkaClient
 * @since 1.0
 */
@InterceptorBean(KafkaClient.class)
@Internal
class KafkaClientIntroductionAdvice implements MethodInterceptor<Object, Object>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientIntroductionAdvice.class);
    private static final ContextSupplier NULL_SUPPLIER = __ -> null;

    private final BeanContext beanContext;
    private final SerdeRegistry serdeRegistry;
    private final ConversionService conversionService;
    private final Map<ProducerKey, ProducerState> producerMap = new ConcurrentHashMap<>();

    /**
     * Creates the introduction advice for the given arguments.
     *
     * @param beanContext       The bean context.
     * @param serdeRegistry     The serde registry
     * @param conversionService The conversion service
     */
    KafkaClientIntroductionAdvice(
            BeanContext beanContext,
            SerdeRegistry serdeRegistry,
            ConversionService conversionService) {
        this.beanContext = beanContext;
        this.serdeRegistry = serdeRegistry;
        this.conversionService = conversionService;
    }

    @Override
    public final Object intercept(MethodInvocationContext<Object, Object> context) {
        if (context.hasAnnotation(KafkaClient.class)) {
            if (!context.hasAnnotation(KafkaClient.class)) {
                throw new IllegalStateException("No @KafkaClient annotation present on method: " + context);
            }
            ProducerState producerState = getProducer(context);

            InterceptedMethod interceptedMethod = InterceptedMethod.of(context, beanContext.getConversionService());
            try {
                Argument<?> returnType = interceptedMethod.returnTypeValue();
                if (Argument.OBJECT_ARGUMENT.equalsType(returnType)) {
                    returnType = Argument.of(RecordMetadata.class);
                }
                switch (interceptedMethod.resultType()) {
                    case COMPLETION_STAGE -> {
                        CompletableFuture<Object> completableFuture = returnCompletableFuture(context, producerState, returnType);
                        return interceptedMethod.handleResult(completableFuture);
                    }
                    case PUBLISHER -> {
                        Flux<Object> returnFlowable = returnPublisher(context, producerState, returnType);
                        return interceptedMethod.handleResult(returnFlowable);
                    }
                    case SYNCHRONOUS -> {
                        return returnSynchronous(context, producerState);
                    }
                    default -> {
                        return interceptedMethod.unsupported();
                    }
                }
            } catch (Exception e) {
                return interceptedMethod.handleException(e);
            }
        } else {
            // can't be implemented so proceed
            return context.proceed();
        }
    }

    private Object returnSynchronous(MethodInvocationContext<Object, Object> context, ProducerState producerState) {
        ReturnType<Object> returnType = context.getReturnType();
        Class<Object> javaReturnType = returnType.getType();
        Argument<Object> returnTypeArgument = returnType.asArgument();
        Object value = producerState.valueSupplier.get(context);
        boolean isReactiveValue = value != null && Publishers.isConvertibleToPublisher(value.getClass());
        if (isReactiveValue) {
            Flux<Object> sendFlowable = buildSendFluxForReactiveValue(context, producerState, returnTypeArgument, value);
            if (Iterable.class.isAssignableFrom(javaReturnType)) {
                return conversionService.convert(sendFlowable.collectList().block(), returnTypeArgument).orElse(null);
            } else if (void.class.isAssignableFrom(javaReturnType)) {
                // a maybe will return null, and not throw an exception
                Mono<Object> maybe = sendFlowable.next();
                return maybe.block();
            } else {
                return conversionService.convert(sendFlowable.blockFirst(), returnTypeArgument).orElse(null);
            }
        } else {
            boolean transactional = producerState.transactional;
            Producer<?, ?> kafkaProducer = producerState.kafkaProducer;
            try {
                if (transactional) {
                    LOG.trace("Beginning transaction for producer: {}", producerState.transactionalId);
                    kafkaProducer.beginTransaction();
                }
                Object returnValue;
                if (producerState.isBatchSend) {
                    Iterable<Object> batchValue;
                    if (value != null && value.getClass().isArray()) {
                        batchValue = Arrays.asList((Object[]) value);
                    } else if (!(value instanceof Iterable iterable)) {
                        batchValue = Collections.singletonList(value);
                    } else {
                        batchValue = iterable;
                    }

                    List<Object> results = new ArrayList<>();
                    for (Object o : batchValue) {
                        ProducerRecord record = buildProducerRecord(context, producerState, o);
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("@KafkaClient method [" + logMethod(context) + "] Sending producer record: " + record);
                        }

                        Object result;
                        if (producerState.maxBlock != null) {
                            result = kafkaProducer.send(record).get(producerState.maxBlock.toMillis(), TimeUnit.MILLISECONDS);
                        } else {
                            result = kafkaProducer.send(record).get();
                        }
                        results.add(result);
                    }
                    returnValue = conversionService.convert(results, returnTypeArgument).orElseGet(() -> {
                        if (javaReturnType == producerState.bodyArgument.getType()) {
                            return value;
                        } else {
                            return null;
                        }
                    });
                } else {
                    ProducerRecord record = buildProducerRecord(context, producerState, value);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("@KafkaClient method [{}] Sending producer record: {}", logMethod(context), record);
                    }

                    Object result;
                    if (producerState.maxBlock != null) {
                        result = kafkaProducer.send(record).get(producerState.maxBlock.toMillis(), TimeUnit.MILLISECONDS);
                    } else {
                        result = kafkaProducer.send(record).get();
                    }
                    returnValue = conversionService.convert(result, returnTypeArgument).orElseGet(() -> {
                        if (javaReturnType == producerState.bodyArgument.getType()) {
                            return value;
                        } else {
                            return null;
                        }
                    });
                }
                if (transactional) {
                    LOG.trace("Committing transaction for producer: {}", producerState.transactionalId);
                    kafkaProducer.commitTransaction();
                }
                return returnValue;
            } catch (Exception e) {
                if (transactional) {
                    LOG.trace("Aborting transaction for producer: {}", producerState.transactionalId);
                    kafkaProducer.abortTransaction();
                }
                throw wrapException(context, e);
            }
        }
    }

    private Flux<Object> returnPublisher(MethodInvocationContext<Object, Object> context, ProducerState producerState, Argument<?> returnType) {
        Object value = producerState.valueSupplier.get(context);
        boolean isReactiveValue = value != null && Publishers.isConvertibleToPublisher(value.getClass());
        Flux<Object> returnFlowable;
        if (isReactiveValue) {
            returnFlowable = buildSendFluxForReactiveValue(context, producerState, returnType, value);
        } else {
            if (producerState.isBatchSend) {
                Object batchValue;
                if (value != null && value.getClass().isArray()) {
                    batchValue = Arrays.asList((Object[]) value);
                } else {
                    batchValue = value;
                }

                Flux<Object> bodyEmitter;
                if (batchValue instanceof Iterable iterable) {
                    bodyEmitter = Flux.fromIterable(iterable);
                } else {
                    bodyEmitter = Flux.just(batchValue);
                }

                returnFlowable = bodyEmitter.flatMap(o -> buildSendFlux(context, producerState, o, returnType));
            } else {
                returnFlowable = buildSendFlux(context, producerState, value, returnType);
            }
        }
        return returnFlowable;
    }

    private CompletableFuture<Object> returnCompletableFuture(MethodInvocationContext<Object, Object> context, ProducerState producerState, Argument<?> returnType) {
        CompletableFuture<Object> completableFuture = new CompletableFuture<>();
        Object value = producerState.valueSupplier.get(context);
        boolean isReactiveValue = value != null && Publishers.isConvertibleToPublisher(value.getClass());
        if (isReactiveValue) {
            Flux sendFlowable = buildSendFluxForReactiveValue(context, producerState, returnType, value);

            if (!Publishers.isSingle(value.getClass())) {
                sendFlowable = sendFlowable.collectList().flux();
            }

            //noinspection SubscriberImplementation
            sendFlowable.subscribe(new Subscriber() {
                boolean completed = false;

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(Object o) {
                    completableFuture.complete(o);
                    completed = true;
                }

                @Override
                public void onError(Throwable t) {
                    completableFuture.completeExceptionally(wrapException(context, t));
                }

                @Override
                public void onComplete() {
                    if (!completed) {
                        // empty publisher
                        completableFuture.complete(null);
                    }
                }
            });
        } else {

            ProducerRecord record = buildProducerRecord(context, producerState, value);
            if (LOG.isTraceEnabled()) {
                LOG.trace("@KafkaClient method [" + logMethod(context) + "] Sending producer record: " + record);
            }

            boolean transactional = producerState.transactional;
            Producer<?, ?> kafkaProducer = producerState.kafkaProducer;
            try {
                if (transactional) {
                    LOG.trace("Beginning transaction for producer: {}", producerState.transactionalId);
                    kafkaProducer.beginTransaction();
                }
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        completableFuture.completeExceptionally(wrapException(context, exception));
                    } else {
                        if (returnType.equalsType(Argument.VOID_OBJECT)) {
                            completableFuture.complete(null);
                        } else {
                            Optional<?> converted = conversionService.convert(metadata, returnType);
                            if (converted.isPresent()) {
                                completableFuture.complete(converted.get());
                            } else if (returnType.getType() == producerState.bodyArgument.getType()) {
                                completableFuture.complete(value);
                            }
                        }
                    }
                });
                if (transactional) {
                    LOG.trace("Committing transaction for producer: {}", producerState.transactionalId);
                    kafkaProducer.commitTransaction();
                }
            } catch (Exception e) {
                if (transactional) {
                    LOG.trace("Aborting transaction for producer: {}", producerState.transactionalId);
                    kafkaProducer.abortTransaction();
                }
                throw e;
            }
        }
        return completableFuture;
    }

    private Mono<RecordMetadata> producerSend(Producer<?, ?> producer, ProducerRecord record) {
        return Mono.create(emitter -> producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                emitter.error(exception);
            } else {
                emitter.success(metadata);
            }
        }));
    }

    @Override
    @PreDestroy
    public final void close() {
        try {
            for (ProducerState producerState : producerMap.values()) {
                try {
                    producerState.kafkaProducer.close();
                } catch (Exception e) {
                    LOG.warn("Error closing Kafka producer: {}", e.getMessage(), e);
                }
            }
        } finally {
            producerMap.clear();
        }
    }

    private Flux<Object> buildSendFlux(MethodInvocationContext<Object, Object> context, ProducerState producerState, Object value, Argument<?> returnType) {
        ProducerRecord record = buildProducerRecord(context, producerState, value);
        return Flux.defer(() -> {
            boolean transactional = producerState.transactional;
            Producer<?, ?> kafkaProducer = producerState.kafkaProducer;
            if (transactional) {
                LOG.trace("Committing transaction for producer: {}", producerState.transactionalId);
                kafkaProducer.beginTransaction();
            }
            Mono<Object> result = producerSend(kafkaProducer, record)
                    .map(metadata -> convertResult(metadata, returnType, value, producerState.bodyArgument))
                    .onErrorMap(e -> wrapException(context, e));
            if (transactional) {
                return addTransactionalProcessing(producerState, result.flux());
            }
            return result;
        });
    }

    private Flux<Object> buildSendFluxForReactiveValue(MethodInvocationContext<Object, Object> context, ProducerState producerState, Argument<?> returnType, Object value) {
        Flux<?> valueFlowable = Flux.from(Publishers.convertPublisher(beanContext.getConversionService(), value, Publisher.class));
        Class<?> javaReturnType = returnType.getType();

        if (Iterable.class.isAssignableFrom(javaReturnType)) {
            returnType = returnType.getFirstTypeVariable().orElse(Argument.OBJECT_ARGUMENT);
        }
        boolean transactional = producerState.transactional;
        Producer<?, ?> kafkaProducer = producerState.kafkaProducer;

        if (transactional) {
            LOG.trace("Beginning transaction for producer: {}", producerState.transactionalId);
            kafkaProducer.beginTransaction();
        }

        Argument<?> finalReturnType = returnType;
        Flux<Object> sendFlowable = valueFlowable.flatMap(o -> {
            ProducerRecord record = buildProducerRecord(context, producerState, o);
            if (LOG.isTraceEnabled()) {
                LOG.trace("@KafkaClient method [{}] Sending producer record: {}", logMethod(context), record);
            }

            return producerSend(kafkaProducer, record)
                    .map(metadata -> convertResult(metadata, finalReturnType, o, producerState.bodyArgument))
                    .onErrorMap(e -> wrapException(context, e));
        });
        if (transactional) {
            sendFlowable = addTransactionalProcessing(producerState, sendFlowable);
        }
        if (producerState.maxBlock != null) {
            sendFlowable = sendFlowable.timeout(producerState.maxBlock);
        }
        return sendFlowable;
    }

    private Flux<Object> addTransactionalProcessing(ProducerState producerState, Flux<Object> sendFlowable) {
        return sendFlowable.doOnError(throwable -> {
                    LOG.trace("Aborting transaction for producer: {}", producerState.transactionalId);
                    producerState.kafkaProducer.abortTransaction();
                })
                .doOnComplete(() -> {
                    LOG.trace("Committing transaction for producer: {}", producerState.transactionalId);
                    producerState.kafkaProducer.commitTransaction();
                });
    }

    private Object convertResult(RecordMetadata metadata, Argument<?> returnType, Object value, Argument<?> valueArgument) {
        if (returnType.isVoid()) {
            return metadata;
        }
        if (RecordMetadata.class.isAssignableFrom(returnType.getType())) {
            return metadata;
        } else if (returnType.getType() == valueArgument.getType()) {
            return value;
        } else {
            return conversionService.convertRequired(metadata, returnType);
        }
    }

    private MessagingClientException wrapException(MethodInvocationContext<Object, Object> context, Throwable exception) {
        return new MessagingClientException(
                "Exception sending producer record for method [" + context + "]: " + exception.getMessage(), exception
        );
    }

    private ProducerRecord<?, ?> buildProducerRecord(MethodInvocationContext<Object, Object> context, ProducerState producerState, Object value) {
        return new ProducerRecord<>(
                producerState.topicSupplier.get(context),
                producerState.partitionSupplier.get(context),
                producerState.timestampSupplier.get(context),
                producerState.keySupplier.get(context),
                value,
                producerState.headersSupplier.get(context)
        );
    }

    @SuppressWarnings("unchecked")
    private ProducerState getProducer(MethodInvocationContext<?, ?> context) {
        ProducerKey key = new ProducerKey(context.getTarget(), context.getExecutableMethod());
        return producerMap.computeIfAbsent(key, producerKey -> {
            String clientId = context.stringValue(KafkaClient.class).orElse(null);

            List<ContextSupplier<Iterable<Header>>> headersSuppliers = new LinkedList<>();
            List<AnnotationValue<MessageHeader>> headers = context.getAnnotationValuesByType(MessageHeader.class);

            if (!headers.isEmpty()) {
                List<Header> kafkaHeaders = new ArrayList<>(headers.size());
                for (AnnotationValue<MessageHeader> header : headers) {
                    String name = header.stringValue("name").orElse(null);
                    String value = header.stringValue().orElse(null);

                    if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(value)) {
                        kafkaHeaders.add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
                    }
                }
                if (!kafkaHeaders.isEmpty()) {
                    headersSuppliers.add(ctx -> kafkaHeaders);
                }
            }

            Argument keyArgument = null;
            Argument bodyArgument = null;
            ContextSupplier<String>[] topicSupplier = new ContextSupplier[1];
            topicSupplier[0] = ctx -> ctx.stringValue(Topic.class).filter(StringUtils::isNotEmpty)
                    .orElseThrow(() -> new MessagingClientException("No topic specified for method: " + context));
            ContextSupplier<Object> keySupplier = NULL_SUPPLIER;
            ContextSupplier<Object> valueSupplier = NULL_SUPPLIER;
            ContextSupplier<Long> timestampSupplier = NULL_SUPPLIER;
            BiFunction<MethodInvocationContext<?, ?>, Producer, Integer> partitionFromProducerFn = (ctx, producer) -> null;
            Argument[] arguments = context.getArguments();
            for (int i = 0; i < arguments.length; i++) {
                int finalI = i;
                Argument<Object> argument = arguments[i];
                if (ProducerRecord.class.isAssignableFrom(argument.getType()) || argument.isAnnotationPresent(MessageBody.class)) {
                    bodyArgument = argument.isAsyncOrReactive() ? argument.getFirstTypeVariable().orElse(Argument.OBJECT_ARGUMENT) : argument;
                    valueSupplier = ctx -> ctx.getParameterValues()[finalI];
                } else if (argument.isAnnotationPresent(KafkaKey.class)) {
                    keyArgument = argument;
                    keySupplier = ctx -> ctx.getParameterValues()[finalI];
                } else if (argument.isAnnotationPresent(Topic.class)) {
                    ContextSupplier<String> prevTopicSupplier = topicSupplier[0];
                    topicSupplier[0] = ctx -> {
                        Object o = ctx.getParameterValues()[finalI];
                        if (o != null) {
                            String topic = o.toString();
                            if (StringUtils.isNotEmpty(topic)) {
                                return topic;
                            }
                        }
                        return prevTopicSupplier.get(ctx);
                    };
                } else if (argument.isAnnotationPresent(KafkaTimestamp.class)) {
                    timestampSupplier = ctx -> {
                        Object o = ctx.getParameterValues()[finalI];
                        if (o instanceof Long l) {
                            return l;
                        }
                        return null;
                    };
                } else if (argument.isAnnotationPresent(KafkaPartition.class)) {
                    partitionFromProducerFn = (ctx, producer) -> {
                        Object o = ctx.getParameterValues()[finalI];
                        if (o != null && Integer.class.isAssignableFrom(o.getClass())) {
                            return (Integer) o;
                        }
                        return null;
                    };
                } else if (argument.isAnnotationPresent(KafkaPartitionKey.class)) {
                    partitionFromProducerFn = (ctx, producer) -> {
                        Object partitionKey = ctx.getParameterValues()[finalI];
                        if (partitionKey != null) {
                            Serializer serializer = serdeRegistry.pickSerializer(argument);
                            if (serializer == null) {
                                serializer = new ByteArraySerializer();
                            }
                            String topic = topicSupplier[0].get(ctx);
                            byte[] partitionKeyBytes = serializer.serialize(topic, partitionKey);
                            return Utils.toPositive(Utils.murmur2(partitionKeyBytes)) % producer.partitionsFor(topic).size();
                        }
                        return null;
                    };
                } else if (argument.isAnnotationPresent(MessageHeader.class)) {
                    final AnnotationMetadata annotationMetadata = argument.getAnnotationMetadata();
                    String name = annotationMetadata
                            .stringValue(MessageHeader.class, "name")
                            .orElseGet(() -> annotationMetadata.stringValue(MessageHeader.class).orElseGet(argument::getName));
                    headersSuppliers.add(ctx -> {
                        Object headerValue = ctx.getParameterValues()[finalI];
                        if (headerValue != null) {
                            Serializer<Object> serializer = serdeRegistry.pickSerializer(argument);
                            if (serializer != null) {
                                try {
                                    return Collections.singleton(new RecordHeader(name, serializer.serialize(null, headerValue)));
                                } catch (Exception e) {
                                    throw new MessagingClientException(
                                            "Cannot serialize header argument [" + argument + "] for method [" + ctx + "]: " + e.getMessage(), e
                                    );
                                }
                            }
                        }
                        return Collections.emptySet();
                    });
                } else {
                    if (argument.isContainerType() && Header.class.isAssignableFrom(argument.getFirstTypeVariable().orElse(Argument.OBJECT_ARGUMENT).getType())) {
                        headersSuppliers.add(ctx -> {
                            Collection<Header> parameterHeaders = (Collection<Header>) ctx.getParameterValues()[finalI];
                            if (parameterHeaders != null) {
                                return parameterHeaders;
                            }
                            return Collections.emptySet();
                        });
                    } else {
                        Class argumentType = argument.getType();
                        if (argumentType == Headers.class || argumentType == RecordHeaders.class) {
                            headersSuppliers.add(ctx -> {
                                Headers parameterHeaders = (Headers) ctx.getParameterValues()[finalI];
                                if (parameterHeaders != null) {
                                    return parameterHeaders;
                                }
                                return Collections.emptySet();
                            });
                        }
                    }
                }
            }
            if (bodyArgument == null) {
                for (int i = 0; i < arguments.length; i++) {
                    int finalI = i;
                    Argument argument = arguments[i];
                    if (!argument.getAnnotationMetadata().hasStereotype(Bindable.class)) {
                        bodyArgument = argument.isAsyncOrReactive() ? argument.getFirstTypeVariable().orElse(Argument.OBJECT_ARGUMENT) : argument;
                        valueSupplier = ctx -> ctx.getParameterValues()[finalI];
                        break;
                    }
                }
                if (bodyArgument == null) {
                    throw new MessagingClientException("No valid message body argument found for method: " + context);
                }
            }

            AbstractKafkaProducerConfiguration configuration;
            if (clientId != null) {
                Optional<KafkaProducerConfiguration> namedConfig = beanContext.findBean(KafkaProducerConfiguration.class, Qualifiers.byName(clientId));
                if (namedConfig.isPresent()) {
                    configuration = namedConfig.get();
                } else {
                    configuration = beanContext.getBean(AbstractKafkaProducerConfiguration.class);
                }
            } else {
                configuration = beanContext.getBean(AbstractKafkaProducerConfiguration.class);
            }

            DefaultKafkaProducerConfiguration<?, ?> newConfiguration = new DefaultKafkaProducerConfiguration<>(configuration);

            Properties newProperties = newConfiguration.getConfig();

            String transactionalId = context.stringValue(KafkaClient.class, "transactionalId").filter(StringUtils::isNotEmpty).orElse(null);

            if (clientId != null) {
                newProperties.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            }
            if (transactionalId != null) {
                newProperties.putIfAbsent(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
            }

            context.getValue(KafkaClient.class, "maxBlock", Duration.class).ifPresent(maxBlock ->
                    newProperties.put(
                            ProducerConfig.MAX_BLOCK_MS_CONFIG,
                            String.valueOf(maxBlock.toMillis())
                    ));

            Integer ack = context.intValue(KafkaClient.class, "acks").orElse(KafkaClient.Acknowledge.DEFAULT);

            if (ack != KafkaClient.Acknowledge.DEFAULT) {
                String acksValue = ack == -1 ? "all" : String.valueOf(ack);
                newProperties.put(
                        ProducerConfig.ACKS_CONFIG,
                        acksValue
                );
            }

            context.findAnnotation(KafkaClient.class).map(ann -> ann.getProperties("properties", "name"))
                    .ifPresent(newProperties::putAll);

            LOG.debug("Creating new KafkaProducer.");

            if (!newProperties.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
                Serializer<?> keySerializer = newConfiguration.getKeySerializer().orElse(null);
                if (keySerializer == null) {
                    if (keyArgument != null) {
                        keySerializer = serdeRegistry.pickSerializer(keyArgument);
                    } else {
                        keySerializer = new ByteArraySerializer();
                    }

                    LOG.debug("Using Kafka key serializer: {}", keySerializer);
                    newConfiguration.setKeySerializer((Serializer) keySerializer);
                }
            }

            boolean isBatchSend = context.isTrue(KafkaClient.class, "batch");

            if (!newProperties.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
                Serializer<?> valueSerializer = newConfiguration.getValueSerializer().orElse(null);

                if (valueSerializer == null) {
                    valueSerializer = serdeRegistry.pickSerializer(isBatchSend ? bodyArgument.getFirstTypeVariable().orElse(bodyArgument) : bodyArgument);

                    LOG.debug("Using Kafka value serializer: {}", valueSerializer);
                    newConfiguration.setValueSerializer((Serializer) valueSerializer);
                }
            }

            Producer<?, ?> producer = beanContext.createBean(Producer.class, newConfiguration);

            boolean transactional = StringUtils.isNotEmpty(transactionalId);
            timestampSupplier = context.isTrue(KafkaClient.class, "timestamp") ? ctx -> System.currentTimeMillis() : timestampSupplier;
            Duration maxBlock = context.getValue(KafkaClient.class, "maxBlock", Duration.class).orElse(null);

            if (transactional) {
                producer.initTransactions();
            }
            ContextSupplier<Collection<Header>> headersSupplier = ctx -> {
                if (headersSuppliers.isEmpty()) {
                    return null;
                }
                List<Header> headerList = new ArrayList<>(headersSuppliers.size());
                for (ContextSupplier<Iterable<Header>> supplier : headersSuppliers) {
                    for (Header header : supplier.get(ctx)) {
                        headerList.add(header);
                    }
                }
                if (headerList.isEmpty()) {
                    return null;
                }
                return headerList;
            };
            BiFunction<MethodInvocationContext<?, ?>, Producer, Integer> finalPartitionFromProducerFn = partitionFromProducerFn;
            ContextSupplier<Integer> partitionSupplier = ctx -> finalPartitionFromProducerFn.apply(ctx, producer);
            return new ProducerState(producer, keySupplier, topicSupplier[0], valueSupplier, timestampSupplier, partitionSupplier, headersSupplier,
                    transactional, transactionalId, maxBlock, isBatchSend, bodyArgument);
        });
    }

    private static String logMethod(ExecutableMethod<?, ?> method) {
        return method.getDeclaringType().getSimpleName() + "#" + method.getName();
    }

    private static final class ProducerState {

        private final Producer<?, ?> kafkaProducer;
        private final ContextSupplier<Object> keySupplier;
        private final ContextSupplier<String> topicSupplier;
        private final ContextSupplier<Object> valueSupplier;
        private final ContextSupplier<Long> timestampSupplier;
        private final ContextSupplier<Integer> partitionSupplier;
        private final ContextSupplier<Collection<Header>> headersSupplier;
        private final boolean transactional;
        private final String transactionalId;
        @Nullable
        private final Duration maxBlock;
        private final boolean isBatchSend;
        private final Argument<?> bodyArgument;

        private ProducerState(Producer<?, ?> kafkaProducer,
                              ContextSupplier<Object> keySupplier,
                              ContextSupplier<String> topicSupplier,
                              ContextSupplier<Object> valueSupplier,
                              ContextSupplier<Long> timestampSupplier,
                              ContextSupplier<Integer> partitionSupplier,
                              ContextSupplier<Collection<Header>> headersSupplier,
                              boolean transactional,
                              @Nullable String transactionalId,
                              @Nullable Duration maxBlock,
                              boolean isBatchSend,
                              @Nullable Argument<?> bodyArgument) {
            this.kafkaProducer = kafkaProducer;
            this.keySupplier = keySupplier;
            this.topicSupplier = topicSupplier;
            this.valueSupplier = valueSupplier;
            this.timestampSupplier = timestampSupplier;
            this.partitionSupplier = partitionSupplier;
            this.headersSupplier = headersSupplier;
            this.transactional = transactional;
            this.transactionalId = transactionalId;
            this.maxBlock = maxBlock;
            this.isBatchSend = isBatchSend;
            this.bodyArgument = bodyArgument;
        }
    }

    private interface ContextSupplier<T> extends Function<MethodInvocationContext<?, ?>, T> {

        default T get(MethodInvocationContext<?, ?> ctx) {
            return apply(ctx);
        }

    }

    /**
     * Key used to cache {@link org.apache.kafka.clients.producer.Producer} instances.
     */
    private static final class ProducerKey {
        private final Object target;
        private final ExecutableMethod<?, ?> method;
        private final int hashCode;

        private ProducerKey(Object target, ExecutableMethod<?, ?> method) {
            this.target = target;
            this.method = method;
            this.hashCode = Objects.hash(target, method);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProducerKey that = (ProducerKey) o;
            return Objects.equals(target, that.target) && Objects.equals(method, that.method);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}
