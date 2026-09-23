from typing import Annotated

from jakarta.inject import Inject
from micronaut.context import ApplicationContext
from micronaut.context.annotation import Property
from micronaut.kafka.docs.Product import Product
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .ProductClient import ProductClient


@Property(name="spec.name", value="ConfigProductListenerTest")
@Property(name="kafka.enabled", value="true")
@MicronautTest(environments=["kafka"])
class ConfigProductListenerTest:
    ctx: Annotated[ApplicationContext, Inject]

    @Test
    def test_send_product(self):
        product = Product("Blue Trainers", 5)
        client = self.ctx.getBean(ProductClient)
        client.send("Nike", product)
