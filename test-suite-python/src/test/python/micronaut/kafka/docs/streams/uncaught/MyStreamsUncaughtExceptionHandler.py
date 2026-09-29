# tag::imports[]
from jakarta.inject import Singleton
from java.lang import Throwable
from micronaut.configuration.kafka.streams.event import BeforeKafkaStreamStart
from micronaut.context.annotation import Requires
from micronaut.context.event import ApplicationEventListener
from org.apache.kafka.streams.errors import StreamsUncaughtExceptionHandler

StreamThreadExceptionResponse = StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
# end::imports[]

from java.lang import RuntimeException


class MyException(RuntimeException):
    ...


@Requires(property="spec.name", value="MyStreamsUncaughtExceptionHandlerTest")
# tag::clazz[]
@Singleton
class MyStreamsUncaughtExceptionHandler(ApplicationEventListener[BeforeKafkaStreamStart], StreamsUncaughtExceptionHandler):

    def __init__(self):
        self.danger_avoided = False

    def onApplicationEvent(self, event: BeforeKafkaStreamStart) -> None:
        event.getKafkaStreams().setUncaughtExceptionHandler(self)

    def handle(self, exception: Throwable) -> StreamThreadExceptionResponse:
        if isinstance(exception.getCause(), MyException):
            self.danger_avoided = True
            return StreamThreadExceptionResponse.REPLACE_THREAD
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
# end::clazz[]
