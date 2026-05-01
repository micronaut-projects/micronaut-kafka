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
package io.micronaut.configuration.kafka.streams;

import io.micronaut.configuration.kafka.streams.event.AfterKafkaStreamsStart;
import io.micronaut.configuration.kafka.streams.event.BeforeKafkaStreamStart;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.*;
import io.micronaut.context.exceptions.DisabledBeanException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.core.util.StringUtils;
import io.micronaut.runtime.graceful.GracefulShutdownCapable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static java.util.function.Predicate.not;

/**
 * A factory that constructs the {@link KafkaStreams} bean.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Factory
@Requires(property = KafkaStreamsConfiguration.ENABLED, notEquals = StringUtils.FALSE, defaultValue = StringUtils.TRUE)
public class KafkaStreamsFactory implements Closeable, GracefulShutdownCapable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsFactory.class);

    private static final String START_KAFKA_STREAMS_PROPERTY = "start-kafka-streams";
    private static final String UNCAUGHT_EXCEPTION_HANDLER_PROPERTY = "uncaught-exception-handler";
    private static final String SHUTDOWN_THREAD_NAME = "micronaut-kafka-streams-shutdown";

    private final Map<KafkaStreams, ConfiguredStreamBuilder> streams = new ConcurrentHashMap<>();
    private final ApplicationEventPublisher eventPublisher;
    private final ApplicationContext applicationContext;
    private final AtomicBoolean applicationShutdownRequested = new AtomicBoolean();
    private final AtomicReference<CompletableFuture<Void>> gracefulShutdown = new AtomicReference<>();

    /**
     * Default constructor.
     *
     * @param eventPublisher The event publisher
     * @param applicationContext The application context
     */
    public KafkaStreamsFactory(ApplicationEventPublisher eventPublisher, ApplicationContext applicationContext) {
        this.eventPublisher = eventPublisher;
        this.applicationContext = applicationContext;
    }

    /**
     * Exposes the {@link ConfiguredStreamBuilder} as a bean.
     *
     * @param configuration The configuration
     * @return The streams builder
     */
    @EachBean(AbstractKafkaStreamsConfiguration.class)
    ConfiguredStreamBuilder streamsBuilder(AbstractKafkaStreamsConfiguration<?, ?> configuration) {
        return new ConfiguredStreamBuilder(configuration.getConfig(), configuration.getName(), configuration.getCloseTimeout());
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
     * @param kStreamsProvider     The KStream definitions
     * @param kTablesProvider      The KTable definitions
     * @param globalKTablesProvider The GlobalKTable definitions
     * @return The {@link KafkaStreams} bean
     */
    @EachBean(ConfiguredStreamBuilder.class)
    @Context
    KafkaStreams kafkaStreams(
            @Parameter String name,
            ConfiguredStreamBuilder builder,
            KafkaClientSupplier kafkaClientSupplier,
            BeanProvider<KStream<?, ?>> kStreamsProvider,
        BeanProvider<KTable<?, ?>> kTablesProvider,
        BeanProvider<GlobalKTable<?, ?>> globalKTablesProvider
    ) {
        KStream<?, ?>[] kStreams = kStreamsProvider.stream().toArray(KStream[]::new);
        // count() forces eager resolution before build() without allocating unused arrays.
        kTablesProvider.stream().count();
        globalKTablesProvider.stream().count();
        Topology topology = builder.build(builder.getConfiguration());
        TopologyDescription topologyDescription = topology.describe();
        if (topologyDescription.subtopologies().isEmpty() && topologyDescription.globalStores().isEmpty()) {
            throw new DisabledBeanException("No topology components registered for stream builder: " + name);
        }
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
            LOG.debug("Initializing Application {} with topology:\n{}", name, topologyDescription.toString());
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
    public CompletableFuture<Void> shutdownGracefully() {
        CompletableFuture<Void> currentShutdown = gracefulShutdown.get();
        if (currentShutdown != null) {
            return currentShutdown;
        }
        CompletableFuture<Void> newShutdown = new CompletableFuture<>();
        if (gracefulShutdown.compareAndSet(null, newShutdown)) {
            CompletableFuture.runAsync(() -> streams.forEach(this::closeStream))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        newShutdown.completeExceptionally(e);
                    } else {
                        newShutdown.complete(null);
                    }
                });
            return newShutdown;
        }
        return gracefulShutdown.get();
    }

    @Override
    public OptionalLong reportActiveTasks() {
        return OptionalLong.of(streams.keySet().stream()
            .filter(stream -> !stream.state().hasCompletedShutdown())
            .count());
    }

    @Override
    @PreDestroy
    public void close() {
        shutdownGracefully().join();
        streams.clear();
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
                        if (response == StreamThreadExceptionResponse.SHUTDOWN_APPLICATION) {
                            requestApplicationShutdown();
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

    private void requestApplicationShutdown() {
        if (!applicationShutdownRequested.compareAndSet(false, true)) {
            return;
        }
        Thread shutdownThread = new Thread(() -> {
            try {
                if (applicationContext.isRunning()) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Stopping Micronaut application because kafka streams requested {}", StreamThreadExceptionResponse.SHUTDOWN_APPLICATION);
                    }
                    applicationContext.stop();
                }
            } catch (Exception e) {
                LOG.warn("Error stopping Micronaut application after kafka streams requested {}", StreamThreadExceptionResponse.SHUTDOWN_APPLICATION, e);
            }
        }, SHUTDOWN_THREAD_NAME);
        shutdownThread.start();
    }

    private void closeStream(KafkaStreams stream, ConfiguredStreamBuilder builder) {
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info("Shutting down kafka stream {} ", builder.getName());
            }
            boolean success = stream.close(builder.getCloseTimeout());
            if (!success) {
                LOG.warn("Timeout was exceeded while attempting to close kafka stream {}", builder.getName());
            }
        } catch (Exception e) {
            // ignore
        }
    }
}
