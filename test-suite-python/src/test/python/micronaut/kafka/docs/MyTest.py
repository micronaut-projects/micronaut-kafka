from abc import ABC, abstractmethod
from queue import Empty, Queue
from typing import Annotated

from jakarta.inject import Inject
from micronaut.configuration.kafka.annotation import KafkaClient, KafkaListener, OffsetReset, Topic
from micronaut.context.annotation import Property, Requires
from micronaut.test.extensions.junit5.annotation import MicronautTest
from micronaut.test.support import TestPropertyProvider
from org.junit.jupiter.api import Disabled, Test, TestInstance

from .AbstractKafkaTest import AbstractKafkaTest


@Requires(property="spec.name", value="MyTest")
@KafkaClient
class MyProducer(ABC):

    @Topic("my-topic")
    @abstractmethod
    def produce(self, message: str) -> None:
        ...


@Requires(property="spec.name", value="MyTest")
@KafkaListener(offsetReset=OffsetReset.EARLIEST)
class MyConsumer:

    def __init__(self):
        self.consumed_messages: Queue[str] = Queue()

    @Topic("my-topic")
    def consume(self, message: str) -> None:
        self.consumed_messages.put(message)

    def await_message(self, timeout: float) -> str | None:
        try:
            return self.consumed_messages.get(timeout=timeout)
        except Empty:
            return None


# TODO(python): TestPropertyProvider.getProperties() is called by Micronaut Test before the
# application context, and with it the GraalPy runtime, exists, so a Python test class cannot
# provide properties yet (and a test class cannot extend AbstractKafkaTest either). The other
# Python tests of this suite get the bootstrap servers from the Java KafkaProperties locator of
# the "kafka" environment instead.
@Disabled("TODO(python): Python test classes cannot implement TestPropertyProvider")
@Property(name="spec.name", value="MyTest")
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MyTest(TestPropertyProvider):
    producer: Annotated[MyProducer, Inject]
    consumer: Annotated[MyConsumer, Inject]

    def getProperties(self) -> dict[str, str]:
        return AbstractKafkaTest().getProperties()

    @Test
    def test_kafka_running(self):
        message = "hello"
        self.producer.produce(message)
        assert self.consumer.await_message(15) == message
