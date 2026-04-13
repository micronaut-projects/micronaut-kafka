package io.micronaut.configuration.kafka.streams.health

import io.micronaut.configuration.kafka.streams.KafkaStreamsFactory
import io.micronaut.management.health.aggregator.HealthAggregator
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.TaskMetadata
import org.apache.kafka.streams.ThreadMetadata
import org.apache.kafka.streams.processor.TaskId
import spock.lang.Specification
import tools.jackson.databind.ObjectMapper

import java.lang.reflect.Method

class KafkaStreamsHealthJacksonSpec extends Specification {

    void "health details remain jackson serializable when task metadata includes task ids"() {
        given:
        KafkaStreamsHealth kafkaStreamsHealth = new KafkaStreamsHealth(Mock(KafkaStreamsFactory), Mock(HealthAggregator))
        TaskMetadata taskMetadata = Mock() {
            taskId() >> new TaskId(1, 5, "my-topology")
            topicPartitions() >> ([new TopicPartition("words", 0)] as Set)
        }
        ThreadMetadata threadMetadata = Mock() {
            threadName() >> "stream-thread-1"
            threadState() >> "RUNNING"
            adminClientId() >> "admin-1"
            consumerClientId() >> "consumer-1"
            restoreConsumerClientId() >> "restore-1"
            producerClientIds() >> ["producer-1"]
            activeTasks() >> ([taskMetadata] as Set)
            standbyTasks() >> ([] as Set)
        }
        KafkaStreams kafkaStreams = Mock() {
            state() >> KafkaStreams.State.RUNNING
            metadataForLocalThreads() >> [threadMetadata]
        }
        ObjectMapper objectMapper = new ObjectMapper()
        Map<String, Object> details = invokeBuildDetails(kafkaStreamsHealth, kafkaStreams)

        when:
        String json = objectMapper.writeValueAsString(details)

        then:
        details['stream-thread-1']['activeTasks']['taskId'] == 'my-topology__1_5'
        json.contains('"taskId":"my-topology__1_5"')
        json.contains('"partition=0, topic=words"')
    }

    private static Map<String, Object> invokeBuildDetails(KafkaStreamsHealth kafkaStreamsHealth, KafkaStreams kafkaStreams) {
        Method buildDetails = KafkaStreamsHealth.getDeclaredMethod("buildDetails", KafkaStreams)
        buildDetails.accessible = true
        (Map<String, Object>) buildDetails.invoke(kafkaStreamsHealth, kafkaStreams)
    }
}
