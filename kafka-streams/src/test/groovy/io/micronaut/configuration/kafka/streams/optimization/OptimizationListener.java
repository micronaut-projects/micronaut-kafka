package io.micronaut.configuration.kafka.streams.optimization;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaListener(offsetReset = OffsetReset.EARLIEST)
public class OptimizationListener {

    private int optimizationOffChangelogMessageCount = 0;

    private int optimizationOnChangelogMessageCount = 0;

    private final String optimizationOffChangelogTopicName =
            OptimizationStream.STREAM_OPTIMIZATION_OFF + "-" +
                    OptimizationStream.OPTIMIZATION_OFF_STORE + "-changelog";

    private final String optimizationOnChangelogTopicName =
            OptimizationStream.STREAM_OPTIMIZATION_ON + "-" +
                    OptimizationStream.OPTIMIZATION_ON_STORE + "-changelog";

    @Topic(optimizationOffChangelogTopicName)
    void optOffCount(@KafkaKey String key, String value) {
        optimizationOffChangelogMessageCount++;
    }

    @Topic(optimizationOnChangelogTopicName)
    void optOnCount(@KafkaKey String key, String value) {
        optimizationOnChangelogMessageCount++;
    }

    public int getOptimizationOffChangelogMessageCount() {
        return optimizationOffChangelogMessageCount;
    }

    public int getOptimizationOnChangelogMessageCount() {
        return optimizationOnChangelogMessageCount;
    }

}
