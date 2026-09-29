from typing import Annotated

from jakarta.inject import Inject
from micronaut.configuration.kafka.annotation import KafkaClient
from micronaut.configuration.kafka.annotation.KafkaClient import Acknowledge
from micronaut.context import BeanContext
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .ProductClient import ProductClient


@MicronautTest
@Property(name="spec.name", value="ProductClientTest")
@Property(name="kafka.enabled", value="false")
class ProductClientTest:
    context: Annotated[BeanContext, Inject]

    @Test
    def test_client_annotation_members(self):
        metadata = self.context.getBeanDefinition(ProductClient).getAnnotationMetadata()
        assert metadata.stringValue(KafkaClient, "id").get() == "product-client"
        assert metadata.intValue(KafkaClient, "acks").getAsInt() == Acknowledge.ALL == KafkaClient.Acknowledge.ALL == -1
