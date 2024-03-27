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

import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.configuration.kafka.config.KafkaHealthConfiguration;
import io.micronaut.configuration.kafka.config.KafkaHealthConfigurationProperties;
import io.micronaut.configuration.kafka.reactor.KafkaReactorUtil;
import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.util.StringUtils;
import io.micronaut.core.util.SupplierUtil;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.kafka.clients.NetworkClientUtils.awaitReady;
import static org.apache.kafka.common.utils.Time.SYSTEM;

/**
 * A {@link HealthIndicator} for Kafka.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Singleton
@Requires(bean = KafkaDefaultConfiguration.class)
@Requires(property = KafkaHealthConfigurationProperties.PREFIX + ".enabled", value = StringUtils.TRUE, defaultValue = StringUtils.TRUE)
public class KafkaHealthIndicator implements HealthIndicator, ClusterResourceListener {
    private static final String ID = "kafka";
    private static final String MIN_INSYNC_REPLICAS_PROPERTY = "min.insync.replicas";
    private static final String REPLICATION_PROPERTY = "offsets.topic.replication.factor";
    private static final String DEFAULT_REPLICATION_PROPERTY = "default.replication.factor";
    private static final String DETAILS_BROKER_ID = "brokerId";
    private static final String DETAILS_CLUSTER_ID = "clusterId";
    private static final String DETAILS_NODES = "nodes";
    private final Supplier<AdminClient> adminClientSupplier;
    private final KafkaDefaultConfiguration defaultConfiguration;

    private final Supplier<NetworkClient> networkClientSupplier;

    private final KafkaHealthConfiguration kafkaHealthConfiguration;

    private String clusterId;

    /**
     * Constructs a new Kafka health indicator for the given arguments.
     *
     * @param beanContext BeanContext
     * @param defaultConfiguration The default configuration
     * @param networkClientCreator Functional interface to create a {@link NetworkClient}.
     * @param kafkaHealthConfiguration Kafka Health indicator configuration
     */
    public KafkaHealthIndicator(BeanContext beanContext,
                                KafkaDefaultConfiguration defaultConfiguration,
                                NetworkClientCreator networkClientCreator,
                                KafkaHealthConfiguration kafkaHealthConfiguration) {
        this.adminClientSupplier = SupplierUtil.memoized(() -> beanContext.getBean(AdminClient.class));
        this.defaultConfiguration = defaultConfiguration;
        this.networkClientSupplier = SupplierUtil.memoized(() -> networkClientCreator.create(this));
        this.kafkaHealthConfiguration = kafkaHealthConfiguration;
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        this.clusterId = Optional.ofNullable(clusterResource).map(ClusterResource::clusterId).orElse(null);
    }

    /**
     * Retrieve the cluster "offsets.topic.replication.factor" for the given {@link Config}, falling back to
     * "default.replication.factor" if required, in order to support Confluent Cloud hosted Kafka.
     *
     * @param config the cluster {@link Config}
     * @return the cluster replication factor, or Integer.MAX_VALUE if none found
     */
    public static int getClusterReplicationFactor(Config config) {
        ConfigEntry ce = Optional.ofNullable(config.get(REPLICATION_PROPERTY)).orElseGet(() -> config.get(DEFAULT_REPLICATION_PROPERTY));
        return ce != null ? Integer.parseInt(ce.value()) : Integer.MAX_VALUE;
    }

    /**
     * Retrieve the cluster "min.insync.replicas" for the given {@link Config}, falling back to
     * "offsets.topic.replication.factor" or "default.replication.factor" if required, in order to
     * support Confluent Cloud hosted Kafka.
     *
     * @param config the cluster {@link Config}
     * @return the optional cluster minimum number of replicas that must acknowledge a write
     */
    public static int getMinNodeCount(Config config) {
        return Optional.ofNullable(config.get(MIN_INSYNC_REPLICAS_PROPERTY)).map(ConfigEntry::value).map(Integer::parseInt)
            .orElseGet(() -> getClusterReplicationFactor(config));
    }

    @Override
    public Flux<HealthResult> getResult() {
        if (kafkaHealthConfiguration.isRestricted()) {
            try {
                NetworkClient client = networkClientSupplier.get();
                return Flux.just(hasReadyNodes(client).orElseGet(() -> waitForLeastLoadedNode(client)));
            } catch (Exception e) {
                return Flux.just(failure(e, Collections.emptyMap()));
            }
        }

        AdminClient adminClient = adminClientSupplier.get();
        DescribeClusterResult result = adminClient.describeCluster(
                new DescribeClusterOptions().timeoutMs(
                        (int) defaultConfiguration.getHealthTimeout().toMillis()
                )
        );

        Mono<String> clusterId = KafkaReactorUtil.fromKafkaFuture(result::clusterId);
        Mono<Collection<Node>> nodes = KafkaReactorUtil.fromKafkaFuture(result::nodes);
        Mono<Node> controller = KafkaReactorUtil.fromKafkaFuture(result::controller);

        return controller.flux().switchMap(node -> {
            String brokerId = node.idString();
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
            DescribeConfigsResult configResult = adminClient.describeConfigs(Collections.singletonList(configResource));
            Mono<Map<ConfigResource, Config>> configs = KafkaReactorUtil.fromKafkaFuture(configResult::all);
            return configs.flux().switchMap(resources -> {
                Config config = resources.get(configResource);
                int minNodeCount = getMinNodeCount(config);
                return nodes.flux().switchMap(nodeList -> clusterId.map(clusterIdString -> {
                    int nodeCount = nodeList.size();
                    return getHealthResult(nodeCount >= minNodeCount, clusterIdString, nodeCount, brokerId);
                }));
            });
        }).onErrorResume(throwable ->
                Mono.just(HealthResult.builder(ID, HealthStatus.DOWN)
                        .exception(throwable).build())
        );
    }

    private static HealthResult getHealthResult(boolean up,
                                                @Nullable String clusterId,
                                                @Nullable Integer nodeCount,
                                                @Nullable String brokerId) {
        Map<String, Object> details = new HashMap<>();
        if (clusterId != null) {
            details.put(DETAILS_CLUSTER_ID, clusterId);
        }
        if (brokerId != null) {
            details.put(DETAILS_BROKER_ID, brokerId);
        }
        if (nodeCount != null) {
            details.put(DETAILS_NODES, nodeCount);
        }
        return result(up,  details).build();
    }

    private static HealthResult getHealthResult(boolean up,
                                                @Nullable String clusterId) {
        return getHealthResult(up, clusterId, null, null);
    }

    private static HealthResult.Builder result(boolean up, Map<String, Object> details) {
        return HealthResult.builder(ID, up ? HealthStatus.UP : HealthStatus.DOWN)
            .details(details);
    }

    private static HealthResult success(Map<String, Object> details) {
        return result(true, details).build();
    }

    private static HealthResult failure(Throwable error, Map<String, Object> details) {
        return result(false, details).exception(error).build();
    }

    private Optional<HealthResult> hasReadyNodes(NetworkClient networkClient) {
        return networkClient.hasReadyNodes(SYSTEM.milliseconds()) ?
            Optional.of(getHealthResult(true, clusterId)) :
            Optional.empty();
    }

    private HealthResult waitForLeastLoadedNode(NetworkClient networkClient) {
        final long requestTimeoutMs = defaultConfiguration.getHealthTimeout().toMillis();
        final Node node = networkClient.leastLoadedNode(SYSTEM.milliseconds());
        try {
            return result(awaitReady(networkClient, node, SYSTEM, requestTimeoutMs), null).build();
        } catch (IOException e) {
            return failure(e, Collections.emptyMap());
        }
    }
}
