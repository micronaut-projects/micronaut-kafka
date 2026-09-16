from time import sleep
from typing import Annotated

from jakarta.inject import Inject
from micronaut.configuration.kafka.admin import KafkaNewTopics
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test


@Property(name="spec.name", value="MyTopicFactoryTest")
@Property(name="kafka.enabled", value="true")
@MicronautTest(environments=["kafka"])
class MyTopicFactoryTest:
    newTopics: Annotated[KafkaNewTopics, Inject]

    @Test
    def test_new_topics(self):
        for _ in range(50):
            if self.are_new_topics_done(self.newTopics):
                break
            sleep(0.1)
        assert self.are_new_topics_done(self.newTopics)
        assert self.newTopics.getResult().numPartitions("my-new-topic-1").get() == 1
        assert self.newTopics.getResult().numPartitions("my-new-topic-2").get() == 2

    # tag::result[]
    def are_new_topics_done(self, new_topics: KafkaNewTopics) -> bool:
        return new_topics.getResult().all().isDone()
    # end::result[]
