from abc import ABC, abstractmethod
from queue import Empty, Queue
from typing import Annotated

from jakarta.inject import Inject
from micronaut.configuration.kafka.annotation import KafkaClient, KafkaListener, OffsetReset, Topic
from micronaut.context.annotation import Property, Requires
from micronaut.test.extensions.junit5.annotation import MicronautTest
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


# TODO(python): Micronaut Test calls TestPropertyProvider.getProperties() before the application
# context, and with it the GraalPy runtime, exists ("GraalPy context has not been initialized"), so a
# Python test class cannot supply the container properties yet. The other Python tests of this suite
# get the bootstrap servers from the Java KafkaTestConfigurer of the "kafka" environment instead.
@Disabled("TODO(python): TestPropertyProvider.getProperties() runs before the GraalPy runtime exists")
@Property(name="spec.name", value="MyTest")
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MyTest(AbstractKafkaTest):
    producer: Annotated[MyProducer, Inject]
    consumer: Annotated[MyConsumer, Inject]

    @Test
    def test_kafka_running(self):
        message = "hello"
        self.producer.produce(message)
        assert self.consumer.await_message(15) == message
