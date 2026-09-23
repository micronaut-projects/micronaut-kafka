from time import sleep
from typing import Annotated

from jakarta.inject import Inject
from micronaut.context import ApplicationContext
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.apache.kafka.streams import KafkaStreams
from org.junit.jupiter.api import Test

from .WordCountClient import WordCountClient
from .WordCountListener import WordCountListener


@Property(name="spec.name", value="WordCountStreamTest")
@Property(name="kafka.enabled", value="true")
@Property(name="micronaut.application.name", value="test-suite-python-word-count-stream")
@Property(name="kafka.streams.default.application.id", value="test-suite-python-word-count-stream-${random.uuid}")
@Property(name="kafka.streams.my-stream.application.id", value="test-suite-python-my-stream-${random.uuid}")
@Property(name="kafka.streams.my-stream.start-kafka-streams", value="false")
@Property(name="kafka.streams.my-other-stream.application.id", value="test-suite-python-my-other-stream-${random.uuid}")
@Property(name="kafka.streams.my-other-stream.start-kafka-streams", value="false")
@MicronautTest(environments=["kafka"])
class WordCountStreamTest:
    ctx: Annotated[ApplicationContext, Inject]
    listener: Annotated[WordCountListener, Inject]

    @Test
    def test_word_counter(self):
        for _ in range(300):
            if self.streams_started():
                break
            sleep(0.1)
        assert self.streams_started()

        client = self.ctx.getBean(WordCountClient)
        client.publish_sentence("test to test for words")

        for _ in range(300):
            if self.counted():
                break
            sleep(0.1)
        assert self.counted()

    def streams_started(self) -> bool:
        states = [streams.state() for streams in self.ctx.getBeansOfType(KafkaStreams)]
        return (len(states) == 3
                and len([state for state in states if state.isRunningOrRebalancing()]) == 1
                and len([state for state in states if state == KafkaStreams.State.CREATED]) == 2)

    def counted(self) -> bool:
        listener = self.listener
        return (len(listener.get_word_counts()) == 4
                and listener.get_count("test") == 2
                and listener.get_count("to") == 1
                and listener.get_count("for") == 1
                and listener.get_count("words") == 1)
