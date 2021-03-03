package io.micronaut.configuration.kafka.streams.optimization;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Context;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

@KafkaListener(offsetReset = OffsetReset.EARLIEST, groupId = "OptimizationListener")
@Context
public class OptimizationListener implements ConsumerRebalanceListener {

    private int optimizationOffChangelogMessageCount = 0;
    private int optimizationOnChangelogMessageCount = 0;

    private final String optimizationOffChangelogTopicName =
            OptimizationStream.STREAM_OPTIMIZATION_OFF + "-" +
                    OptimizationStream.OPTIMIZATION_OFF_STORE + "-changelog";

    private final String optimizationOnChangelogTopicName =
            OptimizationStream.STREAM_OPTIMIZATION_ON + "-" +
                    OptimizationStream.OPTIMIZATION_ON_STORE + "-changelog";

    boolean ready = false;

    public OptimizationListener() {
        System.out.println("Constructed OptimizationListener");
    }

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

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        ready = false;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        ready = true;
    }

    public boolean isReady() {
        return ready;
    }
}
