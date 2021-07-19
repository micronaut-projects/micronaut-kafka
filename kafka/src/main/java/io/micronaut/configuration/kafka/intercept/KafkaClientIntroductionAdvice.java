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
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.bind.annotation.Bindable;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.core.type.ReturnType;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.messaging.annotation.MessageHeader;
import io.micronaut.messaging.exceptions.MessagingClientException;
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

import io.micronaut.core.annotation.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Implementation of the {@link io.micronaut.configuration.kafka.annotation.KafkaClient} advice annotation.
 *
 * @author Graeme Rocher
 * @see io.micronaut.configuration.kafka.annotation.KafkaClient
 * @since 1.0
 */
@InterceptorBean(KafkaClient.class)
public class KafkaClientIntroductionAdvice implements MethodInterceptor<Object, Object>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientIntroductionAdvice.class);

    private final BeanContext beanContext;
    private final SerdeRegistry serdeRegistry;
    private final ConversionService<?> conversionService;
    private final Map<ProducerKey, Producer> producerMap = new ConcurrentHashMap<>();

    /**
     * Creates the introduction advice for the given arguments.
     *
     * @param beanContext       The bean context.
     * @param serdeRegistry     The serde registry
     * @param conversionService The conversion service
     */
    public KafkaClientIntroductionAdvice(
            BeanContext beanContext,
            SerdeRegistry serdeRegistry,
            ConversionService<?> conversionService) {
        this.beanContext = beanContext;
        this.serdeRegistry = serdeRegistry;
        this.conversionService = conversionService;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Object intercept(MethodInvocationContext<Object, Object> context) {

        if (context.hasAnnotation(KafkaClient.class)) {
            if (!context.hasAnnotation(KafkaClient.class)) {
                throw new IllegalStateException("No @KafkaClient annotation present on method: " + context);
            }

            boolean isBatchSend = context.isTrue(KafkaClient.class, "batch");

            String topic = context.stringValue(Topic.class)
                    .orElse(null);

            Argument keyArgument = null;
            Argument bodyArgument = null;
            List<Header> kafkaHeaders = new ArrayList<>();
            List<AnnotationValue<MessageHeader>> headers = context.getAnnotationValuesByType(MessageHeader.class);

            for (AnnotationValue<MessageHeader> header : headers) {
                String name = header.stringValue("name").orElse(null);
                String value = header.stringValue().orElse(null);

                if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(value)) {
                    kafkaHeaders.add(
                            new RecordHeader(
                                    name,
                                    value.getBytes(StandardCharsets.UTF_8)
                            )
                    );
                }
            }

            Argument[] arguments = context.getArguments();
            Object[] parameterValues = context.getParameterValues();
            Object key = null;
            Object value = null;
            Long timestampArgument = null;
            Function<Producer, Integer> partitionSupplier = producer -> null;
            for (int i = 0; i < arguments.length; i++) {
                Argument argument = arguments[i];
                if (ProducerRecord.class.isAssignableFrom(argument.getType()) || argument.isAnnotationPresent(MessageBody.class)) {
                    bodyArgument = argument;
                    value = parameterValues[i];
                } else if (argument.isAnnotationPresent(KafkaKey.class)) {
                   keyArgument = argument;
                   key = parameterValues[i];
                } else if (argument.isAnnotationPresent(Topic.class)) {
                    Object o = parameterValues[i];
                    if (o != null) {
                        topic = o.toString();
                    }
                } else if (argument.isAnnotationPresent(KafkaTimestamp.class)) {
                    Object o = parameterValues[i];
                    if (o instanceof Long) {
                        timestampArgument = (Long) o;
                    }
                } else if (argument.isAnnotationPresent(KafkaPartition.class)) {
                    Object o = parameterValues[i];
                    if (o != null && Integer.class.isAssignableFrom(o.getClass())) {
                        partitionSupplier = __ -> (Integer) o;
                    }
                } else if (argument.isAnnotationPresent(KafkaPartitionKey.class)) {
                    String finalTopic = topic;
                    Object partitionKey = parameterValues[i];
                    if (partitionKey != null) {
                        Serializer serializer = serdeRegistry.pickSerializer(argument);
                        if (serializer == null) {
                            serializer = new ByteArraySerializer();
                        }
                        byte[] partitionKeyBytes = serializer.serialize(finalTopic, parameterValues[i]);
                        partitionSupplier = producer -> Utils.toPositive(Utils.murmur2(partitionKeyBytes)) % producer.partitionsFor(finalTopic).size();
                    }
                } else if (argument.isAnnotationPresent(MessageHeader.class)) {
                    final AnnotationMetadata annotationMetadata = argument.getAnnotationMetadata();
                    String argumentName = argument.getName();
                    String name = annotationMetadata
                            .stringValue(MessageHeader.class, "name")
                            .orElseGet(() ->
                                    annotationMetadata.stringValue(MessageHeader.class).orElse(argumentName));
                    Object v = parameterValues[i];

                    if (v != null) {

                        Serializer serializer = serdeRegistry.pickSerializer(argument);
                        if (serializer != null) {

                            try {
                                kafkaHeaders.add(
                                        new RecordHeader(
                                                name,
                                                serializer.serialize(
                                                        null,
                                                        v
                                                )
                                        )
                                );
                            } catch (Exception e) {
                                throw new MessagingClientException(
                                        "Cannot serialize header argument [" + argument + "] for method [" + context + "]: " + e.getMessage(), e
                                );
                            }
                        }
                    }
                } else {
                    if (argument.isContainerType() && Header.class.isAssignableFrom(argument.getFirstTypeVariable().orElse(Argument.OBJECT_ARGUMENT).getType())) {
                        final Collection<Header> parameterValue = (Collection<Header>) parameterValues[i];
                        if (parameterValue != null) {
                            kafkaHeaders.addAll(parameterValue);
                        }
                    } else {
                        Class argumentType = argument.getType();
                        if (argumentType == Headers.class || argumentType == RecordHeaders.class) {
                            final Headers parameterValue = (Headers) parameterValues[i];
                            if (parameterValue != null) {
                                parameterValue.forEach(kafkaHeaders::add);
                            }
                        }
                    }
                }
            }
            if (bodyArgument == null) {
                for (int i = 0; i < arguments.length; i++) {
                    Argument argument = arguments[i];
                    if (!argument.getAnnotationMetadata().hasStereotype(Bindable.class)) {
                        bodyArgument = argument;
                        value = parameterValues[i];
                        break;
                    }
                }
                if (bodyArgument == null) {
                    throw new MessagingClientException("No valid message body argument found for method: " + context);
                }
            }

            Producer kafkaProducer = getProducer(bodyArgument, keyArgument, context);

            Integer partition = partitionSupplier.apply(kafkaProducer);
            Long timestamp = context.isTrue(KafkaClient.class, "timestamp") ? Long.valueOf(System.currentTimeMillis()) : timestampArgument;
            Duration maxBlock = context.getValue(KafkaClient.class, "maxBlock", Duration.class)
                    .orElse(null);

            boolean isReactiveValue = value != null && Publishers.isConvertibleToPublisher(value.getClass());
            if (StringUtils.isEmpty(topic)) {
                throw new MessagingClientException("No topic specified for method: " + context);
            }

            InterceptedMethod interceptedMethod = InterceptedMethod.of(context);
            try {
                Argument<?> reactiveTypeValue = interceptedMethod.returnTypeValue();
                boolean returnTypeValueVoid = reactiveTypeValue.equalsType(Argument.VOID_OBJECT);
                if (Argument.OBJECT_ARGUMENT.equalsType(reactiveTypeValue)) {
                    reactiveTypeValue = Argument.of(RecordMetadata.class);
                }
                switch (interceptedMethod.resultType()) {
                    case COMPLETION_STAGE:

                        CompletableFuture completableFuture = new CompletableFuture();

                        if (isReactiveValue) {
                            Flux sendFlowable = buildSendFlux(
                                    context,
                                    topic,
                                    kafkaProducer,
                                    kafkaHeaders,
                                    reactiveTypeValue,
                                    key,
                                    partition,
                                    value,
                                    timestamp,
                                    maxBlock);

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

                            ProducerRecord record = buildProducerRecord(topic, partition, kafkaHeaders, key, value, timestamp);
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("@KafkaClient method [" + context + "] Sending producer record: " + record);
                            }

                            Argument finalReturnTypeValue = reactiveTypeValue;
                            Argument finalBodyArgument = bodyArgument;
                            Object finalValue = value;
                            kafkaProducer.send(record, (metadata, exception) -> {
                                if (exception != null) {
                                    completableFuture.completeExceptionally(wrapException(context, exception));
                                } else {
                                    if (!returnTypeValueVoid) {
                                        Optional<?> converted = conversionService.convert(metadata, finalReturnTypeValue);
                                        if (converted.isPresent()) {
                                            completableFuture.complete(converted.get());
                                        } else if (finalReturnTypeValue.getType() == finalBodyArgument.getType()) {
                                            completableFuture.complete(finalValue);
                                        }
                                    } else {
                                        completableFuture.complete(null);
                                    }
                                }
                            });
                        }

                        return interceptedMethod.handleResult(completableFuture);
                    case PUBLISHER:
                        Flux returnFlowable;
                        if (isReactiveValue) {
                            returnFlowable = buildSendFlux(
                                    context,
                                    topic,
                                    kafkaProducer,
                                    kafkaHeaders,
                                    reactiveTypeValue,
                                    key,
                                    partition,
                                    value,
                                    timestamp,
                                    maxBlock);

                        } else {
                            if (isBatchSend) {
                                Object batchValue;
                                if (value != null && value.getClass().isArray()) {
                                    batchValue = Arrays.asList((Object[]) value);
                                } else {
                                    batchValue = value;
                                }

                                Flux<Object> bodyEmitter;
                                if (batchValue instanceof Iterable) {
                                    bodyEmitter = Flux.fromIterable((Iterable) batchValue);
                                } else {
                                    bodyEmitter = Flux.just(batchValue);
                                }

                                String finalTopic = topic;
                                Argument finalBodyArgument = bodyArgument;
                                Object finalKey = key;
                                Integer finalPartition = partition;
                                Argument finalReactiveTypeValue = reactiveTypeValue;
                                returnFlowable = bodyEmitter.flatMap(o ->
                                        buildSendFlux(context, finalTopic, finalBodyArgument, kafkaProducer, kafkaHeaders, finalKey, finalPartition, o, timestamp, finalReactiveTypeValue)
                                );

                            } else {
                                returnFlowable = buildSendFlux(context, topic, bodyArgument, kafkaProducer, kafkaHeaders, key, partition, value, timestamp, reactiveTypeValue);
                            }
                        }
                        return interceptedMethod.handleResult(returnFlowable);
                    case SYNCHRONOUS:
                        ReturnType<Object> returnType = context.getReturnType();
                        Class<Object> javaReturnType = returnType.getType();
                        Argument<Object> returnTypeArgument = returnType.asArgument();
                        if (isReactiveValue) {
                            Flux<Object> sendFlowable = buildSendFlux(
                                    context,
                                    topic,
                                    kafkaProducer,
                                    kafkaHeaders,
                                    returnTypeArgument,
                                    key,
                                    partition,
                                    value,
                                    timestamp,
                                    maxBlock
                            );

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
                            try {
                                if (isBatchSend) {
                                    Iterable batchValue;
                                    if (value != null && value.getClass().isArray()) {
                                        batchValue = Arrays.asList((Object[]) value);
                                    } else if (!(value instanceof Iterable)) {
                                        batchValue = Collections.singletonList(value);
                                    } else {
                                        batchValue = (Iterable) value;
                                    }

                                    List results = new ArrayList();
                                    for (Object o : batchValue) {
                                        ProducerRecord record = buildProducerRecord(topic, partition, kafkaHeaders, key, o, timestamp);

                                        if (LOG.isTraceEnabled()) {
                                            LOG.trace("@KafkaClient method [" + context + "] Sending producer record: " + record);
                                        }

                                        Object result;
                                        if (maxBlock != null) {
                                            result = kafkaProducer.send(record).get(maxBlock.toMillis(), TimeUnit.MILLISECONDS);
                                        } else {
                                            result = kafkaProducer.send(record).get();
                                        }
                                        results.add(result);
                                    }
                                    Argument finalBodyArgument = bodyArgument;
                                    Object finalValue = value;
                                    return conversionService.convert(results, returnTypeArgument).orElseGet(() -> {
                                        if (javaReturnType == finalBodyArgument.getType()) {
                                            return finalValue;
                                        } else {
                                            return null;
                                        }
                                    });
                                }
                                ProducerRecord record = buildProducerRecord(topic, partition, kafkaHeaders, key, value, timestamp);

                                LOG.trace("@KafkaClient method [{}] Sending producer record: {}", context, record);

                                Object result;
                                if (maxBlock != null) {
                                    result = kafkaProducer.send(record).get(maxBlock.toMillis(), TimeUnit.MILLISECONDS);
                                } else {
                                    result = kafkaProducer.send(record).get();
                                }
                                Argument finalBodyArgument = bodyArgument;
                                Object finalValue = value;
                                return conversionService.convert(result, returnTypeArgument).orElseGet(() -> {
                                    if (javaReturnType == finalBodyArgument.getType()) {
                                        return finalValue;
                                    } else {
                                        return null;
                                    }
                                });
                            } catch (Exception e) {
                                throw wrapException(context, e);
                            }
                        }
                    default:
                        return interceptedMethod.unsupported();
                }
            } catch (Exception e) {
                return interceptedMethod.handleException(e);
            }
        } else {
            // can't be implemented so proceed
            return context.proceed();
        }
    }

    @Override
    @PreDestroy
    public final void close() {
        Collection<Producer> kafkaProducers = producerMap.values();
        try {
            for (Producer kafkaProducer : kafkaProducers) {
                try {
                    kafkaProducer.close();
                } catch (Exception e) {
                    LOG.warn("Error closing Kafka producer: {}", e.getMessage(), e);
                }
            }
        } finally {
            producerMap.clear();
        }
    }

    private Flux buildSendFlux(
            MethodInvocationContext<Object, Object> context,
            String topic,
            Argument bodyArgument,
            Producer kafkaProducer,
            List<Header> kafkaHeaders,
            Object key,
            Integer partition,
            Object value,
            Long timestamp,
            Argument reactiveValueType) {
        Flux returnFlowable;
        ProducerRecord record = buildProducerRecord(topic, partition, kafkaHeaders, key, value, timestamp);
        returnFlowable = Flux.create(emitter -> kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                emitter.error(wrapException(context, exception));
            } else {
                if (!reactiveValueType.equalsType(Argument.VOID_OBJECT)) {
                    Optional<?> converted = conversionService.convert(metadata, reactiveValueType);

                    if (converted.isPresent()) {
                        emitter.next(converted.get());
                    } else if (reactiveValueType.getType() == bodyArgument.getType()) {
                        emitter.next(value);
                    }
                }
                emitter.complete();
            }
        }), FluxSink.OverflowStrategy.ERROR);
        return returnFlowable;
    }

    private Flux<Object> buildSendFlux(
            MethodInvocationContext<Object, Object> context,
            String topic,
            Producer kafkaProducer,
            List<Header> kafkaHeaders,
            Argument<?> returnType,
            Object key,
            Integer partition,
            Object value,
            Long timestamp,
            Duration maxBlock) {
        Flux<?> valueFlowable = Flux.from(Publishers.convertPublisher(value, Publisher.class));
        Class<?> javaReturnType = returnType.getType();

        if (Iterable.class.isAssignableFrom(javaReturnType)) {
            javaReturnType = returnType.getFirstTypeVariable().orElse(Argument.OBJECT_ARGUMENT).getType();
        }

        Class<?> finalJavaReturnType = javaReturnType;
        Flux<Object> sendFlowable = valueFlowable.flatMap(o -> {
            ProducerRecord record = buildProducerRecord(topic, partition, kafkaHeaders, key, o, timestamp);

            LOG.trace("@KafkaClient method [{}] Sending producer record: {}", context, record);

            //noinspection unchecked
            return Flux.create(emitter -> kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    emitter.error(wrapException(context, exception));
                } else {
                    if (RecordMetadata.class.isAssignableFrom(finalJavaReturnType)) {
                        emitter.next(metadata);
                    } else if (finalJavaReturnType.isInstance(o)) {
                        emitter.next(o);
                    } else {
                        Optional converted = conversionService.convert(metadata, finalJavaReturnType);
                        if (converted.isPresent()) {
                            emitter.next(converted.get());
                        }
                    }

                    emitter.complete();
                }
            }), FluxSink.OverflowStrategy.BUFFER);
        });

        if (maxBlock != null) {
            sendFlowable = sendFlowable.timeout(maxBlock);
        }
        return sendFlowable;
    }

    private MessagingClientException wrapException(MethodInvocationContext<Object, Object> context, Throwable exception) {
        return new MessagingClientException(
                "Exception sending producer record for method [" + context + "]: " + exception.getMessage(), exception
        );
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord buildProducerRecord(String topic, Integer partition, List<Header> kafkaHeaders, Object key, Object value, Long timestamp) {
        return new ProducerRecord(
                topic,
                partition,
                timestamp,
                key,
                value,
                kafkaHeaders.isEmpty() ? null : kafkaHeaders
        );
    }

    @SuppressWarnings("unchecked")
    private Producer getProducer(Argument bodyArgument, @Nullable Argument keyArgument, AnnotationMetadata metadata) {
        Class keyType = keyArgument != null ? keyArgument.getType() : byte[].class;
        String clientId = metadata.stringValue(KafkaClient.class).orElse(null);
        ProducerKey key = new ProducerKey(keyType, bodyArgument.getType(), clientId);
        return producerMap.computeIfAbsent(key, producerKey -> {
            String producerId = producerKey.id;
            AbstractKafkaProducerConfiguration configuration;
            if (producerId != null) {
                Optional<KafkaProducerConfiguration> namedConfig = beanContext.findBean(KafkaProducerConfiguration.class, Qualifiers.byName(producerId));
                if (namedConfig.isPresent()) {
                    configuration = namedConfig.get();
                } else {
                    configuration = beanContext.getBean(AbstractKafkaProducerConfiguration.class);
                }
            } else {
                configuration = beanContext.getBean(AbstractKafkaProducerConfiguration.class);
            }

            DefaultKafkaProducerConfiguration<?, ?> newConfiguration = new DefaultKafkaProducerConfiguration<>(
                    configuration
            );

            Properties newProperties = newConfiguration.getConfig();

            if (clientId != null) {
                newProperties.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            }

            metadata.getValue(KafkaClient.class, "maxBlock", Duration.class).ifPresent(maxBlock ->
                    newProperties.put(
                            ProducerConfig.MAX_BLOCK_MS_CONFIG,
                            String.valueOf(maxBlock.toMillis())
                    ));

            Integer ack = metadata.intValue(KafkaClient.class, "acks").orElse(KafkaClient.Acknowledge.DEFAULT);

            if (ack != KafkaClient.Acknowledge.DEFAULT) {
                String acksValue = ack == -1 ? "all" : String.valueOf(ack);
                newProperties.put(
                        ProducerConfig.ACKS_CONFIG,
                        acksValue
                );
            }

            metadata.findAnnotation(KafkaClient.class).map(ann ->
                    ann.getProperties("properties", "name")
            ).ifPresent(newProperties::putAll);

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

            if (!newProperties.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
                Serializer<?> valueSerializer = newConfiguration.getValueSerializer().orElse(null);

                if (valueSerializer == null) {
                    boolean batch = metadata.isTrue(KafkaClient.class, "batch");
                    valueSerializer = serdeRegistry.pickSerializer(batch ? bodyArgument.getFirstTypeVariable().orElse(bodyArgument) : bodyArgument);

                    LOG.debug("Using Kafka value serializer: {}", valueSerializer);
                    newConfiguration.setValueSerializer((Serializer) valueSerializer);
                }
            }

            return beanContext.createBean(Producer.class, newConfiguration);
        });
    }

    /**
     * Key used to cache {@link org.apache.kafka.clients.producer.Producer} instances.
     */
    private static final class ProducerKey {
        final Class keyType;
        final Class valueType;
        final String id;

        ProducerKey(Class keyType, Class valueType, String id) {
            this.keyType = keyType;
            this.valueType = valueType;
            this.id = id;
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
            return Objects.equals(keyType, that.keyType) &&
                    Objects.equals(valueType, that.valueType) &&
                    Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(keyType, valueType, id);
        }
    }
}
