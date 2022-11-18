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
package io.micronaut.configuration.kafka.streams.health;

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.configuration.kafka.streams.KafkaStreamsFactory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Internal;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.aggregator.HealthAggregator;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.reactivestreams.Publisher;

import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@link HealthIndicator} for Kafka Streams.
 *
 * @author Christian Oestreich
 * @since 2.0.1
 */
@Singleton
@Requires(classes = HealthIndicator.class)
@Requires(property = KafkaStreamsHealth.ENABLED_PROPERTY, value = "true", defaultValue = "true")
public class KafkaStreamsHealth implements HealthIndicator {

    public static final String ENABLED_PROPERTY = AbstractKafkaConfiguration.PREFIX + ".health.streams.enabled";

    private static final String NAME = "kafkaStreams";
    private static final String METADATA_PARTITIONS = "partitions";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsHealth.class);

    private final KafkaStreamsFactory kafkaStreamsFactory;
    private final HealthAggregator<?> healthAggregator;

    /**
     * Constructor for the health check.
     *
     * @param kafkaStreamsFactory The stream factory to get streams from
     * @param healthAggregator    Health aggregator
     */
    public KafkaStreamsHealth(final KafkaStreamsFactory kafkaStreamsFactory, final HealthAggregator<?> healthAggregator) {
        this.kafkaStreamsFactory = kafkaStreamsFactory;
        this.healthAggregator = healthAggregator;
    }

    /**
     * Get the health result of the streams.  Will attempt to interrogate
     * details of each stream as well.  The application.id will be used
     * for each configured stream as the primary health check name.
     *
     * @return Health Result Aggregate
     */

    @Override
    public Publisher<HealthResult> getResult() {
        Flux<HealthResult> kafkaStreamHealth = Flux.fromIterable(this.kafkaStreamsFactory.getStreams().keySet())
                .map(kafkaStreams -> Pair.of(getApplicationId(kafkaStreams), kafkaStreams))
                .flatMap(pair -> Flux.just(pair)
                        .filter(p -> p.getValue().state().isRunningOrRebalancing())
                        .map(p -> HealthResult.builder(p.getKey(), HealthStatus.UP)
                                .details(buildDetails(p.getValue())))
                        .defaultIfEmpty(HealthResult.builder(pair.getKey(), HealthStatus.DOWN)
                                .details(buildDownDetails(pair.getValue().state(), pair.getKey())))
                        .onErrorResume(e -> Flux.just(HealthResult.builder(pair.getKey(), HealthStatus.DOWN)
                                .details(buildDownDetails(e.getMessage(), pair.getValue().state(), pair.getKey())))))
                .map(HealthResult.Builder::build);
        return healthAggregator.aggregate(NAME, kafkaStreamHealth);
    }

    /**
     * Build down details for the stream down.
     *
     * @param state The stream state
     * @return Map of details messages
     */
    private Map<String, String> buildDownDetails(KafkaStreams.State state, String streamId) {
        return buildDownDetails("Processor appears to be down", state, streamId);
    }

    /**
     * Build down details for the stream down.
     *
     * @param message Down message
     * @param state The stream state
     * @param streamId The stream ID
     * @return Map of details messages
     */
    private Map<String, String> buildDownDetails(String message, KafkaStreams.State state, String streamId) {
        LOG.debug("Reporting health DOWN. Kafka stream {} in state {} is down: {}", streamId, state, message);
        final Map<String, String> details = new HashMap<>();
        details.put("threadState", state.name());
        details.put("error", message);
        return details;
    }

    /**
     * Build up details for the stream.
     *
     * @param kafkaStreams The stream to build details for
     * @return Map of details
     */
    private Map<String, Object> buildDetails(KafkaStreams kafkaStreams) {
        final Map<String, Object> streamDetails = new HashMap<>();

        if (kafkaStreams.state().isRunningOrRebalancing()) {
            for (org.apache.kafka.streams.ThreadMetadata metadata : kafkaStreams.metadataForLocalThreads()) {
                final Map<String, Object> threadDetails = new HashMap<>();
                threadDetails.put("threadName", metadata.threadName());
                threadDetails.put("threadState", metadata.threadState());
                threadDetails.put("adminClientId", metadata.adminClientId());
                threadDetails.put("consumerClientId", metadata.consumerClientId());
                threadDetails.put("restoreConsumerClientId", metadata.restoreConsumerClientId());
                threadDetails.put("producerClientIds", metadata.producerClientIds());
                threadDetails.put("activeTasks", taskDetails(metadata.activeTasks()));
                threadDetails.put("standbyTasks", taskDetails(metadata.standbyTasks()));
                streamDetails.put(metadata.threadName(), threadDetails);
            }
        } else {
            streamDetails.put("error", "The processor is down");
        }
        return streamDetails;
    }

    /**
     * Derive the application.id from the stream.  Will use the following order to attempt to get name for stream.
     * <p>
     * application.id -> client.id -> threadName -> kafkaStream.toString()
     *
     * @param kafkaStreams The kafka stream
     * @return Application id
     */
    private String getApplicationId(final KafkaStreams kafkaStreams) {
        try {
            ConfiguredStreamBuilder configuredStreamBuilder = kafkaStreamsFactory.getStreams().get(kafkaStreams);
            if (configuredStreamBuilder != null) {
                Properties configuration = configuredStreamBuilder.getConfiguration();
                return (String) configuration.getOrDefault(StreamsConfig.APPLICATION_ID_CONFIG, configuration.getProperty(StreamsConfig.CLIENT_ID_CONFIG));
            } else {
                return getDefaultStreamName(kafkaStreams);
            }
        } catch (Exception e) {
            return getDefaultStreamName(kafkaStreams);
        }
    }

    /**
     * Get default name if application.id and client.id are not present.
     *
     * @param kafkaStreams the kafka stream
     * @return The name
     */
    private static String getDefaultStreamName(final KafkaStreams kafkaStreams) {
        return Optional.ofNullable(kafkaStreams)
                .filter(kafkaStreams1 -> kafkaStreams1.state().isRunningOrRebalancing())
                .map(KafkaStreams::metadataForLocalThreads)
                .map(Collection::stream)
                .flatMap(Stream::findFirst)
                .map(ThreadMetadata::threadName)
                .orElse("unidentified");
    }

    /**
     * Get task details for the kafka stream.
     *
     * @param taskMetadataSet the task metadata
     * @return map of details
     */
    private static Map<String, Object> taskDetails(Set<TaskMetadata> taskMetadataSet) {
        final Map<String, Object> details = new HashMap<>();
        for (TaskMetadata taskMetadata : taskMetadataSet) {
            details.put("taskId", taskMetadata.taskId());
            if (details.containsKey(METADATA_PARTITIONS)) {
                @SuppressWarnings("unchecked")
                List<String> partitionsInfo = (List<String>) details.get(METADATA_PARTITIONS);
                partitionsInfo.addAll(addPartitionsInfo(taskMetadata));
            } else {
                details.put(METADATA_PARTITIONS, addPartitionsInfo(taskMetadata));
            }
        }
        return details;
    }

    /**
     * Add partition details if available.
     *
     * @param metadata Task metadata
     * @return List of partition and topic details
     */
    private static List<String> addPartitionsInfo(TaskMetadata metadata) {
        return metadata.topicPartitions().stream()
                .map(p -> "partition=" + p.partition() + ", topic=" + p.topic())
                .toList();
    }

    /**
     * Internal only class due to missing "Tuple" like interface in Java 8.
     *
     * @param <K> The key
     * @param <V> The value
     */
    @Internal
    private static class Pair<K, V> {
        private final K key;
        private final V value;

        public Pair(final K key, final V value) {
            this.key = key;
            this.value = value;
        }

        public static <K, V> Pair<K, V> of(K key, V value) {
            return new Pair<>(key, value);
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
