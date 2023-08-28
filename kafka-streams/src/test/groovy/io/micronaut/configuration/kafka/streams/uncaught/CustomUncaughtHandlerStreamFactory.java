package io.micronaut.configuration.kafka.streams.uncaught;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.concurrent.atomic.AtomicBoolean;

@Requires(property = "spec.name", value = "CustomUncaughtHandlerSpec")
@Factory
public class CustomUncaughtHandlerStreamFactory {
    public static final String CUSTOM_HANDLER = "my-custom-handler";
    public static final String CUSTOM_HANDLER_INPUT = "my-custom-handler-input";
    public static final String CUSTOM_HANDLER_OUTPUT = "my-custom-handler-output";

    @Singleton
    @Named(CUSTOM_HANDLER)
    KStream<String, String> createMyCustomHandlerStream(@Named(CUSTOM_HANDLER) ConfiguredStreamBuilder builder) {
        final AtomicBoolean flag = new AtomicBoolean();
        final KStream<String, String> source = builder
            .stream(CUSTOM_HANDLER_INPUT, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(value -> {
                // Throw a custom exception only the first time we find "ERROR"
                if (flag.compareAndSet(false, true) && value.equals("ERROR")) {
                    throw new MyException("Uh-oh! Prepare for an uncaught exception");
                }
                return value.toUpperCase();
            });
        source.to(CUSTOM_HANDLER_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
        return source;
    }
}
