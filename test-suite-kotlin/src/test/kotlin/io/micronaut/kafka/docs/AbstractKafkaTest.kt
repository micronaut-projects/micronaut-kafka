package io.micronaut.kafka.docs

import io.micronaut.test.support.TestPropertyProvider
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.*

/**
 * @see <a href="https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers">Singleton containers</a>
 */
abstract class AbstractKafkaTest : TestPropertyProvider {

    companion object {
        var MY_KAFKA: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))

        init {
            MY_KAFKA.start()
        }
    }

    override fun getProperties(): MutableMap<String, String> {
        while(!MY_KAFKA.isRunning()) {
            MY_KAFKA.start()
        }
        return Collections.singletonMap(
            "kafka.bootstrap.servers", MY_KAFKA.getBootstrapServers()
        )
    }
}
