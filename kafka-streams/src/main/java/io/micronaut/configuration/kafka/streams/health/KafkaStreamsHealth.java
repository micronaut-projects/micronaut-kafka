/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.streams.health;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.configuration.kafka.streams.KafkaStreamsConfiguration;
import io.micronaut.configuration.kafka.streams.KafkaStreamsFactory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.aggregator.HealthAggregator;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.reactivex.Flowable;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.reactivestreams.Publisher;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link HealthIndicator} for Kafka Streams.
 *
 * @author Christian Oestreich
 * @since 2.0.1
 */
@Singleton
@Requires(classes = HealthIndicator.class)
@Requires(property = KafkaStreamsConfiguration.PREFIX + ".health.enabled", value = "true", defaultValue = "true")
public class KafkaStreamsHealth implements HealthIndicator {

    private static final String NAME = "kafkaStreams";

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
        Flowable<HealthResult> kafkaStreamHealth = Flowable.fromIterable(kafkaStreamsFactory.getStreams().keySet())
                .map(kafkaStreams -> {
                    String applicationId = getApplicationId(kafkaStreams);
                    try {
                        if (kafkaStreams.state().isRunning()) {
                            return HealthResult.builder(applicationId, HealthStatus.UP)
                                    .details(buildDetails(kafkaStreams))
                                    .build();
                        } else {
                            return HealthResult.builder(applicationId, HealthStatus.DOWN)
                                    .details(buildDetails(kafkaStreams))
                                    .build();
                        }
                    } catch (Exception e) {
                        return HealthResult.builder(applicationId, HealthStatus.DOWN)
                                .details(buildDownDetails(e, kafkaStreams.state()))
                                .build();
                    }
                });

        return healthAggregator.aggregate(NAME, kafkaStreamHealth);

    }

    /**
     * Build down details for the stream down.
     *
     * @return Map of details
     */
    private Map<String, String> buildDownDetails(Exception e, KafkaStreams.State state) {
        final Map<String, String> details = new HashMap<>();
        details.put("state", state.name());
        details.put("error", e.getMessage());
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

        if (kafkaStreams.state().isRunning()) {
            for (ThreadMetadata metadata : kafkaStreams.localThreadsMetadata()) {
                streamDetails.put("threadName", metadata.threadName());
                streamDetails.put("threadState", metadata.threadState());
                streamDetails.put("adminClientId", metadata.adminClientId());
                streamDetails.put("consumerClientId", metadata.consumerClientId());
                streamDetails.put("restoreConsumerClientId", metadata.restoreConsumerClientId());
                streamDetails.put("producerClientIds", metadata.producerClientIds());
                streamDetails.put("activeTasks", taskDetails(metadata.activeTasks()));
                streamDetails.put("standbyTasks", taskDetails(metadata.standbyTasks()));
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
    private String getDefaultStreamName(final KafkaStreams kafkaStreams) {
        if (kafkaStreams != null) {
            return Optional.ofNullable(kafkaStreams.localThreadsMetadata())
                    .map(threadMetadataSet -> threadMetadataSet.stream().findFirst())
                    .map(Optional::get)
                    .map(ThreadMetadata::threadName)
                    .orElse(kafkaStreams.toString());
        }
        return "unidentified";
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
            if (details.containsKey("partitions")) {
                @SuppressWarnings("unchecked")
                List<String> partitionsInfo = (List<String>) details.get("partitions");
                partitionsInfo.addAll(addPartitionsInfo(taskMetadata));
            } else {
                details.put("partitions", addPartitionsInfo(taskMetadata));
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
                .collect(Collectors.toList());
    }
}
