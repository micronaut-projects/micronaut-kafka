package io.micronaut.configuration.kafka.streams.health

import io.micronaut.configuration.kafka.streams.KafkaStreamsFactory
import io.micronaut.management.health.aggregator.HealthAggregator
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.TaskMetadata
import org.apache.kafka.streams.ThreadMetadata
import org.apache.kafka.streams.processor.TaskId
import spock.lang.Specification

class KafkaStreamsHealthTaskMetadataSpec extends Specification {

    void "health details expose task ids as strings and preserve task metadata per task"() {
        given:
        KafkaStreamsHealth kafkaStreamsHealth = new KafkaStreamsHealth(Mock(KafkaStreamsFactory), Mock(HealthAggregator))
        TaskId firstTaskId = new TaskId(1, 5, "my-topology")
        TaskId secondTaskId = new TaskId(1, 6, "my-topology")
        TaskMetadata firstTaskMetadata = Mock() {
            taskId() >> firstTaskId
            topicPartitions() >> ([new TopicPartition("words", 0)] as Set)
        }
        TaskMetadata secondTaskMetadata = Mock() {
            taskId() >> secondTaskId
            topicPartitions() >> ([new TopicPartition("words", 1)] as Set)
        }
        ThreadMetadata threadMetadata = Mock() {
            threadName() >> "stream-thread-1"
            threadState() >> "RUNNING"
            adminClientId() >> "admin-1"
            consumerClientId() >> "consumer-1"
            restoreConsumerClientId() >> "restore-1"
            producerClientIds() >> ["producer-1"]
            activeTasks() >> ([firstTaskMetadata, secondTaskMetadata] as Set)
            standbyTasks() >> ([] as Set)
        }
        KafkaStreams kafkaStreams = Mock() {
            state() >> KafkaStreams.State.RUNNING
            metadataForLocalThreads() >> [threadMetadata]
        }
        when:
        Map<String, Object> details = kafkaStreamsHealth.buildDetails(kafkaStreams)
        List<Map<String, Object>> activeTasks = (List<Map<String, Object>>) details['stream-thread-1']['activeTasks']

        then:
        activeTasks*.taskId as Set == [firstTaskId.toString(), secondTaskId.toString()] as Set
        activeTasks.every { it.taskId instanceof String }
        activeTasks.find { it.taskId == firstTaskId.toString() }?.partitions == ['partition=0, topic=words']
        activeTasks.find { it.taskId == secondTaskId.toString() }?.partitions == ['partition=1, topic=words']
    }
}
