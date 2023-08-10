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
package io.micronaut.configuration.kafka.health;

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.context.annotation.Requires;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.*;
import org.apache.kafka.common.utils.LogContext;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Predicate.not;
import static org.apache.kafka.clients.ClientUtils.createChannelBuilder;
import static org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses;
import static org.apache.kafka.clients.CommonClientConfigs.*;
import static org.apache.kafka.clients.NetworkClientUtils.awaitReady;
import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * A restricted {@link HealthIndicator} for Kafka.
 *
 * <p>Unlike the regular {@link KafkaHealthIndicator}, which requires cluster-wide permissions in
 * order to get information about the nodes in the Kafka cluster, the restricted health indicator
 * only checks basic connectivity. It will return health status {@code UP} if it can connect to at
 * least one node in the cluster.</p>
 *
 * @author Guillermo Calvo
 * @since 4.1
 */
@Singleton
@Requires(property = AbstractKafkaConfiguration.PREFIX + ".health.enabled", value = "true", defaultValue = "true")
@Requires(property = AbstractKafkaConfiguration.PREFIX + ".health.restricted", value = "true", defaultValue = "false")
public class RestrictedKafkaHealthIndicator implements HealthIndicator, ClusterResourceListener {

    private static final String ID = "kafka";
    private static final String METRICS_NAMESPACE = "kafka.health-indicator.client";
    private static final String DEFAULT_CLIENT_ID = "health-indicator-client";
    private static final String LOG_PREFIX = "[HealthIndicator clientId=%s] ";

    private final KafkaDefaultConfiguration defaultConfiguration;
    private final AbstractConfig config;
    private final String clientId;
    private final LogContext logContext;
    private final NetworkClient networkClient;
    private String clusterId;

    /**
     * Constructs a restricted health indicator for the given arguments.
     *
     * @param defaultConfiguration The default configuration
     */
    public RestrictedKafkaHealthIndicator(KafkaDefaultConfiguration defaultConfiguration) {
        this.defaultConfiguration = defaultConfiguration;
        config = new AdminClientConfig(defaultConfiguration.getConfig());
        clientId = Optional.ofNullable(config.getString(CLIENT_ID_CONFIG)).filter(not(String::isEmpty)).orElse(DEFAULT_CLIENT_ID);
        logContext = new LogContext(String.format(LOG_PREFIX, clientId));
        networkClient = createNetworkClient();
    }

    @Override
    public void onUpdate(ClusterResource cluster) {
        this.clusterId = Optional.ofNullable(cluster).map(ClusterResource::clusterId).orElse(null);
    }

    @Override
    public Flux<HealthResult> getResult() {
        try {
            return Flux.just(hasReadyNodes().orElseGet(this::waitForLeastLoadedNode));
        } catch (Exception e) {
            return Flux.just(failure(e));
        }
    }

    private Optional<HealthResult> hasReadyNodes() {
        return networkClient.hasReadyNodes(SYSTEM.milliseconds()) ?
            Optional.of(success()) :
            Optional.empty();
    }

    private HealthResult waitForLeastLoadedNode() {
        final long requestTimeoutMs = defaultConfiguration.getHealthTimeout().toMillis();
        final Node node = networkClient.leastLoadedNode(SYSTEM.milliseconds());
        try {
            return result(awaitReady(networkClient, node, SYSTEM, requestTimeoutMs)).build();
        } catch (IOException e) {
            return failure(e);
        }
    }

    private HealthResult.Builder result(boolean up) {
        return HealthResult.builder(ID, up ? HealthStatus.UP : HealthStatus.DOWN)
            .details(Map.of("clusterId", Optional.ofNullable(clusterId).orElse("")));
    }

    private HealthResult success() {
        return result(true).build();
    }

    private HealthResult failure(Throwable error) {
        return result(false).exception(error).build();
    }

    private NetworkClient createNetworkClient() {
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
            return new NetworkClient(selector, metadata(), clientId, 1,
                reconnectBackoff, reconnectBackoffMax, socketSendBuffer, socketReceiveBuffer,
                (int) HOURS.toMillis(1), connectionSetupTimeout, connectionSetupTimeoutMax,
                SYSTEM, true, new ApiVersions(), logContext);
        } catch (Throwable e) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed to create new NetworkClient", e);
        }
    }

    private ClusterResourceListeners clusterListeners() {
        final ClusterResourceListeners clusterListeners = new ClusterResourceListeners();
        clusterListeners.maybeAdd(this);
        return clusterListeners;
    }

    private Metadata metadata() {
        final long refreshBackoff = config.getLong(RETRY_BACKOFF_MS_CONFIG);
        final long metadataExpire = config.getLong(METADATA_MAX_AGE_CONFIG);
        final List<String> urls = config.getList(BOOTSTRAP_SERVERS_CONFIG);
        final String clientDnsLookup = config.getString(CLIENT_DNS_LOOKUP_CONFIG);
        final Metadata metadata = new Metadata(refreshBackoff, metadataExpire, logContext, clusterListeners());
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
