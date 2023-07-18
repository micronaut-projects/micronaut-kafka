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
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.concurrent.atomic.AtomicBoolean;

@Requires(property = "spec.name", value = "UncaughtExceptionsSpec")
@Factory
public class OnErrorStreamFactory {

    public static final String ON_ERROR_NO_CONFIG = "on-error-no-config";
    public static final String ON_ERROR_NO_CONFIG_INPUT = "on-error-no-config-input";
    public static final String ON_ERROR_NO_CONFIG_OUTPUT = "on-error-no-config-output";
    public static final String ON_ERROR_REPLACE = "on-error-replace";
    public static final String ON_ERROR_REPLACE_INPUT = "on-error-replace-input";
    public static final String ON_ERROR_REPLACE_OUTPUT = "on-error-replace-output";

    @Singleton
    @Named(ON_ERROR_NO_CONFIG)
    KStream<String, String> createOnErrorNoConfigStream(@Named(ON_ERROR_NO_CONFIG) ConfiguredStreamBuilder builder) {
        final KStream<String, String> source = builder
            .stream(ON_ERROR_NO_CONFIG_INPUT, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(makeErrorValueMapper());
        source.to(ON_ERROR_REPLACE_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
        return source;
    }

    @Singleton
    @Named(ON_ERROR_REPLACE)
    KStream<String, String> createOnErrorReplaceThreadStream(@Named(ON_ERROR_REPLACE) ConfiguredStreamBuilder builder) {
        final KStream<String, String> source = builder
            .stream(ON_ERROR_REPLACE_INPUT, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(makeErrorValueMapper());
        source.to(ON_ERROR_REPLACE_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
        return source;
    }

    static ValueMapper<String, String> makeErrorValueMapper() {
        final AtomicBoolean flag = new AtomicBoolean();
        return value -> {
            // Throw an exception only the first time we find "ERROR"
            if (flag.compareAndSet(false, true) && value.equals("ERROR")) {
                throw new IllegalStateException("Uh-oh! Prepare for an uncaught exception");
            }
            return value.toUpperCase();
        };
    }
}
