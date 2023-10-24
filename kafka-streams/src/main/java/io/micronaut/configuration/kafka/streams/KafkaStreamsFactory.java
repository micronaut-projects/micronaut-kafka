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
package io.micronaut.configuration.kafka.streams;

import io.micronaut.configuration.kafka.streams.event.AfterKafkaStreamsStart;
import io.micronaut.configuration.kafka.streams.event.BeforeKafkaStreamStart;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Secondary;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventPublisher;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Arrays.asList;
import static java.util.function.Predicate.not;

/**
 * A factory that constructs the {@link KafkaStreams} bean.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Factory
public class KafkaStreamsFactory implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsFactory.class);

    private static final String START_KAFKA_STREAMS_PROPERTY = "start-kafka-streams";
    private static final String UNCAUGHT_EXCEPTION_HANDLER_PROPERTY = "uncaught-exception-handler";

    private final Map<KafkaStreams, ConfiguredStreamBuilder> streams = new ConcurrentHashMap<>();

    private final ApplicationEventPublisher eventPublisher;

    @Value("${kafka.streams-close-seconds:3}")
    private int closeWaitSeconds;

    /**
     * Default constructor.
     *
     * @param eventPublisher The event publisher
     */
    public KafkaStreamsFactory(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    /**
     * Exposes the {@link ConfiguredStreamBuilder} as a bean.
     *
     * @param configuration The configuration
     * @return The streams builder
     */
    @EachBean(AbstractKafkaStreamsConfiguration.class)
    ConfiguredStreamBuilder streamsBuilder(AbstractKafkaStreamsConfiguration<?, ?> configuration) {
        return new ConfiguredStreamBuilder(configuration.getConfig());
    }

    /**
     * Get configured stream and builder for the stream.
     *
     * @return Map of streams to builders
     */
    public Map<KafkaStreams, ConfiguredStreamBuilder> getStreams() {
        return streams;
    }

    /**
     * Builds the default {@link KafkaStreams} bean from the configuration and the supplied {@link ConfiguredStreamBuilder}.
     *
     * @param name                 The configuration name
     * @param builder              The builder
     * @param kafkaClientSupplier  The kafka client supplier used to create consumers and producers in the streams app
     * @param kStreams             The KStream definitions
     * @return The {@link KafkaStreams} bean
     */
    @EachBean(ConfiguredStreamBuilder.class)
    @Context
    KafkaStreams kafkaStreams(
            @Parameter String name,
            ConfiguredStreamBuilder builder,
            KafkaClientSupplier kafkaClientSupplier,
            KStream<?, ?>... kStreams
    ) {
        Topology topology = builder.build(builder.getConfiguration());
        KafkaStreams kafkaStreams = new KafkaStreams(
                topology,
                builder.getConfiguration(),
                kafkaClientSupplier
        );
        makeUncaughtExceptionHandler(builder.getConfiguration()).ifPresent(kafkaStreams::setUncaughtExceptionHandler);
        final String startKafkaStreamsValue = builder.getConfiguration().getProperty(
            START_KAFKA_STREAMS_PROPERTY, Boolean.TRUE.toString());
        final boolean startKafkaStreams = Boolean.parseBoolean(startKafkaStreamsValue);
        if (startKafkaStreams) {
            eventPublisher.publishEvent(new BeforeKafkaStreamStart(kafkaStreams, kStreams));
        }
        streams.put(kafkaStreams, builder);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initializing Application {} with topology:\n{}", name, topology.describe().toString());
        }

        if (startKafkaStreams) {
            kafkaStreams.start();
            eventPublisher.publishEvent(new AfterKafkaStreamsStart(kafkaStreams, kStreams));
        }
        return kafkaStreams;
    }

    /**
     * Create the interactive query service bean.
     *
     * @return Rhe {@link InteractiveQueryService} bean
     */
    @Singleton
    InteractiveQueryService interactiveQueryService() {
        return new InteractiveQueryService(streams.keySet());
    }

    /**
     * Provide a default kafka client supplier which is overridable.
     * @return DefaultKafkaClientSupplier
     */
    @Singleton
    @Secondary
    KafkaClientSupplier kafkaClientSupplier() {
        return new DefaultKafkaClientSupplier();
    }

    @Override
    @PreDestroy
    public void close() {
        for (KafkaStreams stream : streams.keySet()) {
            try {
                stream.close(Duration.ofSeconds(closeWaitSeconds));
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Make an uncaught exception handler for a given kafka streams configuration.
     *
     * @param properties The kafka streams configuration.
     * @return An optional exception handler if {@code uncaught-exception-handler} was configured.
     */
    Optional<StreamsUncaughtExceptionHandler> makeUncaughtExceptionHandler(Properties properties) {
        return Optional.ofNullable(properties.getProperty(UNCAUGHT_EXCEPTION_HANDLER_PROPERTY))
            .filter(not(String::isBlank))
            .map(action -> {
                try {
                    final StreamThreadExceptionResponse response = StreamThreadExceptionResponse.valueOf(action.toUpperCase());
                    return exception -> {
                        if (LOG.isWarnEnabled()) {
                            LOG.warn("Responding with {} to unexpected exception thrown by kafka stream thread", response, exception);
                        }
                        return response;
                    };
                } catch (IllegalArgumentException e) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Ignoring illegal exception handler: {}. Please use one of: {}", action,
                            asList(StreamThreadExceptionResponse.values()));
                    }
                    return null;
                }
            });
    }
}
