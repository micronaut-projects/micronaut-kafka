from micronaut.test.support import TestPropertyProvider
from org.testcontainers.kafka import KafkaContainer


class AbstractKafkaTest(TestPropertyProvider):
    """
    See https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
    """
    MY_KAFKA: KafkaContainer = KafkaContainer("apache/kafka:4.2.0")

    def getProperties(self) -> dict[str, str]:
        if not AbstractKafkaTest.MY_KAFKA.isRunning():
            AbstractKafkaTest.MY_KAFKA.start()
        return {"kafka.bootstrap.servers": AbstractKafkaTest.MY_KAFKA.getBootstrapServers()}
