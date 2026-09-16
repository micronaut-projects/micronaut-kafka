from typing import Annotated

from jakarta.inject import Inject
from micronaut.context import ApplicationContext
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .ProductClient import ProductClient


@Property(name="spec.name", value="QuickstartTest")
@Property(name="kafka.enabled", value="true")
@MicronautTest(environments=["kafka"])
class QuickstartTest:
    applicationContext: Annotated[ApplicationContext, Inject]

    @Test
    def test_send_product(self):
        # tag::quickstart[]
        client = self.applicationContext.getBean(ProductClient).asPolyglotValue()
        client.send_product("Nike", "Blue Trainers")
        # end::quickstart[]
