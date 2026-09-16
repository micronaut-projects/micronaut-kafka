from typing import Annotated

from jakarta.inject import Singleton
from java.util import Collections
from micronaut.configuration.kafka.streams.event import BeforeKafkaStreamStart
from micronaut.context.annotation import Requires, Value
from micronaut.runtime.event.annotation import EventListener
from org.apache.kafka.clients.admin import AdminClient, AdminClientConfig, NewTopic


# The Java, Kotlin and Groovy suites create the topics from a BootstrapPropertySourceLocator; the
# bootstrap context is created before the GraalPy runtime, so the Python suite creates them right
# before the Kafka Streams are started instead.
@Requires(property="spec.name", value="WordCountStreamTest")
@Singleton
class KafkaTestInitializer:

    def __init__(self, bootstrap_servers: Annotated[str, Value("${kafka.bootstrap.servers}")]):
        self.admin_props = Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)

    @EventListener
    def initialize_topics(self, event: BeforeKafkaStreamStart) -> None:
        self.create_topics([
            self.configure_topic(topic_name, 1, 1)
            for topic_name in ["streams-plaintext-input", "named-word-count-input", "my-other-stream", "no-op-input"]
        ])

    @staticmethod
    def configure_topic(name: str, num_partitions: int, replication_factor: int) -> NewTopic:
        return NewTopic(name, num_partitions, replication_factor)

    def create_topics(self, topics_to_create: list[NewTopic]) -> None:
        admin = AdminClient.create(self.admin_props)
        try:
            existing_topics = admin.listTopics().names().get()
            new_topics = [new_topic for new_topic in topics_to_create if not existing_topics.contains(new_topic.name())]
            admin.createTopics(new_topics).all().get()
        finally:
            admin.close()
