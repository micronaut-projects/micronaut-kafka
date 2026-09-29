from time import sleep
from typing import Annotated

from jakarta.inject import Inject
from micronaut.context.annotation import Property
from micronaut.kafka.docs.Products import Products
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .ProductListener import ProductListener


@MicronautTest(environments=["kafka"])
@Property(name="spec.name", value="ConsumerSeekAwareTest")
class ConsumerSeekAwareTest:
    consumer: Annotated[ProductListener, Inject]

    @Test
    def test_product_listener(self):
        for _ in range(100):
            if self.seeked():
                break
            sleep(0.1)
        assert self.seeked()

    def seeked(self) -> bool:
        processed = [product.name for product in self.consumer.processed]
        return (Products.PRODUCT_0.name not in processed
                and Products.PRODUCT_1.name in processed)
