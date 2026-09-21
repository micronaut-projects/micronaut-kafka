from time import sleep
from typing import Annotated

from jakarta.inject import Inject
from micronaut.context import ApplicationContext
from micronaut.context.annotation import Property
from micronaut.kafka.docs.Product import Product
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .ProductClient import ProductClient
from .QuantityListener import QuantityListener


@Property(name="spec.name", value="SendToProductListenerTest")
@Property(name="kafka.enabled", value="true")
@MicronautTest(environments=["kafka"])
class SendToProductListenerTest:
    ctx: Annotated[ApplicationContext, Inject]
    listener: Annotated[QuantityListener, Inject]

    @Test
    def test_send_product(self):
        product = Product("Blue Trainers", 5)
        client = self.ctx.getBean(ProductClient)
        client.send("Nike", product)
        for _ in range(100):
            if self.listener.quantity == 5:
                break
            sleep(0.1)
        assert self.listener.quantity == 5
