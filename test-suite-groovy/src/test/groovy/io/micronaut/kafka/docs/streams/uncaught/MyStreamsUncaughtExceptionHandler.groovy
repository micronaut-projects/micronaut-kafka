package io.micronaut.kafka.docs.streams.uncaught

// tag::imports[]
import io.micronaut.configuration.kafka.streams.event.BeforeKafkaStreamStart
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
// end::imports[]

@Requires(property = "spec.name", value = "MyStreamsUncaughtExceptionHandlerTest")
// tag::clazz[]
@Singleton
class MyStreamsUncaughtExceptionHandler
    implements ApplicationEventListener<BeforeKafkaStreamStart>, StreamsUncaughtExceptionHandler {

    boolean dangerAvoided = false

    @Override
    void onApplicationEvent(BeforeKafkaStreamStart event) {
        event.kafkaStreams.setUncaughtExceptionHandler(this)
    }

    @Override
    StreamThreadExceptionResponse handle(Throwable exception) {
        if (exception.cause instanceof MyException) {
            dangerAvoided = true
            return StreamThreadExceptionResponse.REPLACE_THREAD
        }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
    }
}
// end::clazz[]

class MyException extends RuntimeException {
    MyException(String message) {
        super(message)
    }
}
