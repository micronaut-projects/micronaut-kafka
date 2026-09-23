from typing import Annotated

from jakarta.inject import Inject
from micronaut.context import BeanContext
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .MessageClientFallback import MessageClientFallback


@MicronautTest
@Property(name="spec.name", value="MessageClientFallbackTest")
@Property(name="kafka.enabled", value="false")
class MessageClientFallbackTest:
    context: Annotated[BeanContext, Inject]

    @Test
    def test_context_contains_fallback_bean(self):
        bean = self.context.getBean(MessageClientFallback)
        assert bean is not None
        try:
            bean.send("message")
        except NotImplementedError:
            pass
        else:
            assert False, "expected NotImplementedError"
