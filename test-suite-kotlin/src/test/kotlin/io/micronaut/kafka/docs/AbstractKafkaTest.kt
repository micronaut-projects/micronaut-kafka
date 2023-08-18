package io.micronaut.kafka.docs

import io.micronaut.test.support.TestPropertyProvider
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * @see <a href="https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers">Singleton containers</a>
 */
abstract class AbstractKafkaTest : TestPropertyProvider {

    companion object {
        var MY_KAFKA: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
    }

    override fun getProperties(): MutableMap<String, String> {
        if(!MY_KAFKA.isRunning) {
            MY_KAFKA.start()
        }
        return mutableMapOf(
            "kafka.bootstrap.servers" to MY_KAFKA.bootstrapServers
        )
    }
}
