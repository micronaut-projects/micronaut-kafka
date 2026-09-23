from time import sleep
from typing import Annotated

from jakarta.inject import Inject
from micronaut.context import ApplicationContext
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .WordCountListener import WordCountListener
from .WordCounterClient import WordCounterClient


@Property(name="spec.name", value="WordCounterTest")
@Property(name="kafka.enabled", value="true")
@Property(name="kafka.streams.enabled", value="false")
@MicronautTest(environments=["kafka"])
class WordCounterTest:
    ctx: Annotated[ApplicationContext, Inject]
    listener: Annotated[WordCountListener, Inject]

    @Test
    def test_word_counter(self):
        client = self.ctx.getBean(WordCounterClient)
        client.send("test to test for words")
        for _ in range(100):
            if self.counted():
                break
            sleep(0.1)
        assert self.counted()

    def counted(self) -> bool:
        word_count = self.listener.word_count
        return (len(word_count) == 4
                and word_count.get("test") == 2
                and word_count.get("to") == 1
                and word_count.get("for") == 1
                and word_count.get("words") == 1)
