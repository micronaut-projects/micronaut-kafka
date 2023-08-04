package io.micronaut.configuration.kafka.streams.uncaught;

// tag::imports[]
import io.micronaut.configuration.kafka.streams.event.BeforeKafkaStreamStart;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import jakarta.inject.Singleton;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
// end::imports[]

@Requires(property = "spec.name", value = "CustomUncaughtHandlerSpec")
// tag::clazz[]
@Singleton
public class MyStreamsUncaughtExceptionHandler
    implements ApplicationEventListener<BeforeKafkaStreamStart>, StreamsUncaughtExceptionHandler {

    boolean dangerAvoided = false;

    @Override
    public void onApplicationEvent(BeforeKafkaStreamStart event) {
        event.getKafkaStreams().setUncaughtExceptionHandler(this);
    }

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        if (exception.getCause() instanceof MyException) {
            this.dangerAvoided = true;
            return StreamThreadExceptionResponse.REPLACE_THREAD;
        }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
// end::clazz[]

class MyException extends RuntimeException{
    public MyException(String message) {
        super(message);
    }
}
