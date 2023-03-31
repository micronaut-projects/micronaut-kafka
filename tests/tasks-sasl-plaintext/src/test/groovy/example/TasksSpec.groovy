package example

import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@MicronautTest
class TasksSpec extends Specification implements TestPropertyProvider {

    @Shared
    @AutoCleanup
    KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
            .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf")
            .withCopyFileToContainer(MountableFile.forClasspathResource("/example/kafka_server_jaas.conf"), "/etc/kafka/kafka_server_jaas.conf")

    @Inject
    @Client("/")
    HttpClient client

    PollingConditions pollingConditions = new PollingConditions(initialDelay: 2, timeout: 100)

    void 'should process tasks'() {
        expect:
        pollingConditions.eventually {
            client.toBlocking().retrieve("/tasks/processed-count", Integer) > 3
        }
    }

    @Override
    Map<String, String> getProperties() {
        kafkaContainer.start()
        ['kafka.bootstrap.servers': kafkaContainer.bootstrapServers]
    }
}
