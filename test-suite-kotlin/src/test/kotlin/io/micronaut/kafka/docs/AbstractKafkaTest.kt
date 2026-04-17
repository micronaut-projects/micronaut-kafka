package io.micronaut.kafka.docs

import io.micronaut.test.support.TestPropertyProvider
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * @see <a href="https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers">Singleton containers</a>
 */
abstract class AbstractKafkaTest : TestPropertyProvider {

    companion object {
        private val KAFKA_IMAGE: DockerImageName = DockerImageName.parse("apache/kafka:4.2.0")
        var MY_KAFKA: KafkaContainer = KafkaContainer(KAFKA_IMAGE)
    }

    override fun getProperties(): MutableMap<String, String> {
        if (!MY_KAFKA.isRunning) {
            MY_KAFKA.start()
        }

        val properties = mutableMapOf(
            "kafka.bootstrap.servers" to MY_KAFKA.bootstrapServers
        )
        return properties
    }
}
