package io.micronaut.configuration.kafka.streams.health.serde

import io.micronaut.configuration.kafka.streams.AbstractTestContainersSpec
import io.micronaut.json.JsonMapper
import org.apache.kafka.streams.processor.TaskId

class TaskIdSerdeSpec extends AbstractTestContainersSpec {

    void "should serialize TaskId"() {
        given:
        def jsonMapper = context.getBean(JsonMapper)
        def taskId = new TaskId(1, 5, "my-topology")
        def expectedTaskStr = taskId.toString()

        when:
        String json = jsonMapper.writeValueAsString(taskId)

        then:
        json != null
        json == "\"${expectedTaskStr}\""
    }

    void "should deserialize TaskId"() {
        given:
        def jsonMapper = context.getBean(JsonMapper)
        def expectedTaskStr = "my-topology__1_5"
        String serializedString = "\"${expectedTaskStr}\""

        when:
        TaskId deserializedTaskId = jsonMapper.readValue(serializedString, TaskId)

        then:
        deserializedTaskId != null
        deserializedTaskId.toString() == expectedTaskStr
    }
}
