package io.micronaut.configuration.kafka.streams.uncaught;

// tag::imports[]
import io.micronaut.configuration.kafka.streams.event.BeforeKafkaStreamStart;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import jakarta.inject.Singleton;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
// end::imports[]

@Requires(property = "do.not.enable", value = "just for documentation purposes")
// tag::clazz[]
@Singleton
public class MyStreamsUncaughtExceptionHandler
    implements ApplicationEventListener<BeforeKafkaStreamStart>, StreamsUncaughtExceptionHandler {

    @Override
    public void onApplicationEvent(BeforeKafkaStreamStart event) {
        event.getKafkaStreams().setUncaughtExceptionHandler(this);
    }

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
// end::imports[]
