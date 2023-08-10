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
import io.micronaut.configuration.kafka.reactor.KafkaReactorUtil;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link HealthIndicator} for Kafka.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Singleton
@Requires(beans = AdminClient.class)
@Requires(missingBeans = RestrictedKafkaHealthIndicator.class)
@Requires(property = AbstractKafkaConfiguration.PREFIX + ".health.enabled", value = "true", defaultValue = "true")
public class KafkaHealthIndicator implements HealthIndicator {

    private static final String ID = "kafka";
    private static final String MIN_INSYNC_REPLICAS_PROPERTY = "min.insync.replicas";
    private static final String REPLICATION_PROPERTY = "offsets.topic.replication.factor";
    private static final String DEFAULT_REPLICATION_PROPERTY = "default.replication.factor";
    private final AdminClient adminClient;
    private final KafkaDefaultConfiguration defaultConfiguration;

    /**
     * Constructs a new Kafka health indicator for the given arguments.
     *
     * @param adminClient          The admin client
     * @param defaultConfiguration The default configuration
     */
    public KafkaHealthIndicator(AdminClient adminClient, KafkaDefaultConfiguration defaultConfiguration) {
        this.adminClient = adminClient;
        this.defaultConfiguration = defaultConfiguration;
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
                    HealthResult.Builder builder;
                    if (nodeCount >= minNodeCount) {
                        builder = HealthResult.builder(ID, HealthStatus.UP);
                    } else {
                        builder = HealthResult.builder(ID, HealthStatus.DOWN);
                    }
                    return builder
                            .details(CollectionUtils.mapOf(
                                    "brokerId", brokerId,
                                    "clusterId", clusterIdString,
                                    "nodes", nodeCount
                            )).build();
                }));
            });
        }).onErrorResume(throwable ->
                Mono.just(HealthResult.builder(ID, HealthStatus.DOWN)
                        .exception(throwable).build())
        );
    }

}
