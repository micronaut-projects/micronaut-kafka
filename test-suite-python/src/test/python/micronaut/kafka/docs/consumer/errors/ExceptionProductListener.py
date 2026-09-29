from micronaut.configuration.kafka.annotation import ErrorStrategy, ErrorStrategyValue, KafkaListener
from micronaut.context.annotation import Requires


class MyException(Exception):
    pass


class MySecondException(Exception):
    pass


@Requires(property="spec.name", value="ExceptionProductListenerTest")
# tag::annotation[]
@KafkaListener(
    value="myGroup",
    errorStrategy=ErrorStrategy(
        value=ErrorStrategyValue.RETRY_ON_ERROR,
        retryDelay="50ms",
        retryCount=3,
        exceptionTypes=[MyException, MySecondException]
    )
)
# end::annotation[]
class ExceptionProductListener:
    pass
