/*
 * Copyright 2017-2023 original authors
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
package io.micronaut.configuration.kafka.health;

import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.core.annotation.NonNull;
import jakarta.inject.Singleton;
import jdk.jfr.Experimental;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Predicate.not;
import static org.apache.kafka.clients.ClientUtils.createChannelBuilder;
import static org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses;
import static org.apache.kafka.clients.CommonClientConfigs.*;
import static org.apache.kafka.clients.CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;
import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * Default implementation of {@link NetworkClientCreator}. Based on {@link org.apache.kafka.clients.admin.KafkaAdminClient} createInternal method.
 *
 * @since 5.1.0
 */
@Experimental
@Singleton
public class DefaultNetworkClientCreator implements NetworkClientCreator {
    private static final String LOG_PREFIX = "[HealthIndicator clientId=%s] ";
    private static final String DEFAULT_CLIENT_ID = "health-indicator-client";

    private static final String METRICS_NAMESPACE = "kafka.health-indicator.client";

    private final LogContext logContext;
    private final AbstractConfig config;

    private final String clientId;

    public DefaultNetworkClientCreator(KafkaDefaultConfiguration defaultConfiguration) {
        AbstractConfig config = new AdminClientConfig(defaultConfiguration.getConfig());
        String clientId = Optional.ofNullable(config.getString(CLIENT_ID_CONFIG)).filter(not(String::isEmpty)).orElse(DEFAULT_CLIENT_ID);
        this.config = config;
        this.logContext = new LogContext(String.format(LOG_PREFIX, clientId));
        this.clientId = clientId;
    }

    @Override
    @NonNull
    public NetworkClient create(@NonNull ClusterResourceListener... listeners) {
        final long reconnectBackoff = config.getLong(RECONNECT_BACKOFF_MS_CONFIG);
        final long reconnectBackoffMax = config.getLong(RECONNECT_BACKOFF_MAX_MS_CONFIG);
        final int socketSendBuffer = config.getInt(SEND_BUFFER_CONFIG);
        final int socketReceiveBuffer = config.getInt(RECEIVE_BUFFER_CONFIG);
        final long connectionSetupTimeout = config.getLong(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG);
        final long connectionSetupTimeoutMax = config.getLong(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG);
        Metrics metrics = null;
        Selector selector = null;
        ChannelBuilder channelBuilder = null;
        try {
            metrics = metrics();
            channelBuilder = createChannelBuilder(config, SYSTEM, logContext);
            selector = selector(metrics, channelBuilder);
            return new NetworkClient(
                selector,
                metadata(listeners),
                clientId,
                1,
                reconnectBackoff,
                reconnectBackoffMax,
                socketSendBuffer,
                socketReceiveBuffer,
                (int) HOURS.toMillis(1),
                connectionSetupTimeout,
                connectionSetupTimeoutMax,
                SYSTEM,
                true,
                new ApiVersions(),
                logContext,
                MetadataRecoveryStrategy.NONE
            );
        } catch (Throwable e) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed to create new NetworkClient", e);
        }
    }

    private ClusterResourceListeners clusterListeners(ClusterResourceListener... listeners) {
        final ClusterResourceListeners clusterListeners = new ClusterResourceListeners();
        for (ClusterResourceListener listener : listeners) {
            clusterListeners.maybeAdd(listener);
        }
        return clusterListeners;
    }

    private Metadata metadata(ClusterResourceListener... listeners) {
        final long refreshBackoff = config.getLong(RETRY_BACKOFF_MS_CONFIG);
        final long refreshBackoffMax = config.getLong(RETRY_BACKOFF_MAX_MS_CONFIG);
        final long metadataExpire = config.getLong(METADATA_MAX_AGE_CONFIG);
        final List<String> urls = config.getList(BOOTSTRAP_SERVERS_CONFIG);
        final String clientDnsLookup = config.getString(CLIENT_DNS_LOOKUP_CONFIG);
        final Metadata metadata = new Metadata(refreshBackoff, refreshBackoffMax, metadataExpire, logContext, clusterListeners(listeners));
        metadata.bootstrap(parseAndValidateAddresses(urls, clientDnsLookup));
        return metadata;
    }

    private Metrics metrics() {
        final int samples = config.getInt(METRICS_NUM_SAMPLES_CONFIG);
        final long timeWindow = config.getLong(METRICS_SAMPLE_WINDOW_MS_CONFIG);
        final String recordLevel = config.getString(METRICS_RECORDING_LEVEL_CONFIG);
        final MetricConfig metricConfig = new MetricConfig()
            .samples(samples)
            .timeWindow(timeWindow, MILLISECONDS)
            .recordLevel(Sensor.RecordingLevel.forName(recordLevel))
            .tags(singletonMap("client-id", clientId));
        final MetricsContext context = new KafkaMetricsContext(
            METRICS_NAMESPACE, config.originalsWithPrefix(METRICS_CONTEXT_PREFIX));
        return new Metrics(metricConfig, metricsReporters(clientId, config), SYSTEM, context);
    }

    private Selector selector(Metrics metrics, ChannelBuilder channelBuilder) {
        final long connectionMaxIdle = config.getLong(CONNECTIONS_MAX_IDLE_MS_CONFIG);
        return new Selector(connectionMaxIdle, metrics, SYSTEM, DEFAULT_CLIENT_ID, channelBuilder, logContext);
    }
}
