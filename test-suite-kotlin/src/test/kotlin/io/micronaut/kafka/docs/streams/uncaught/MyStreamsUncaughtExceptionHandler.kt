package io.micronaut.kafka.docs.streams.uncaught

// tag::imports[]
import io.micronaut.configuration.kafka.streams.event.BeforeKafkaStreamStart
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
// end::imports[]

@Requires(property = "spec.name", value = "MyStreamsUncaughtExceptionHandlerTest")
// tag::clazz[]
@Singleton
class MyStreamsUncaughtExceptionHandler :
    ApplicationEventListener<BeforeKafkaStreamStart>, StreamsUncaughtExceptionHandler {

    var dangerAvoided = false

    override fun onApplicationEvent(event: BeforeKafkaStreamStart) {
        event.kafkaStreams.setUncaughtExceptionHandler(this)
    }

    override fun handle(exception: Throwable): StreamThreadExceptionResponse {
        if (exception.cause is MyException) {
            dangerAvoided = true
            return StreamThreadExceptionResponse.REPLACE_THREAD
        }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
    }
}
// end::clazz[]

class MyException(message: String) : RuntimeException(message)
