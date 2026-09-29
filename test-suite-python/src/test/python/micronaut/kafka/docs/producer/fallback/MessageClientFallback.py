# tag::imports[]
from jakarta.inject import Singleton
from micronaut.context.annotation import Replaces, Requires
from micronaut.core.util import StringUtils

from .MessageClient import MessageClient
# end::imports[]


@Requires(property="spec.name", value="MessageClientFallbackTest")
# tag::clazz[]
@Requires(property="kafka.enabled", notEquals=StringUtils.TRUE, defaultValue=StringUtils.TRUE)  # <1>
@Replaces(MessageClient)  # <2>
@Singleton
class MessageClientFallback(MessageClient):  # <3>

    def send(self, message: str) -> None:
        raise NotImplementedError()  # <4>
# end::clazz[]
