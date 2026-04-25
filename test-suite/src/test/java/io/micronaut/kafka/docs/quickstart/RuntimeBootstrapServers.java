package io.micronaut.kafka.docs.quickstart;

// tag::imports[]
import io.micronaut.context.ApplicationContextBuilder;
import io.micronaut.context.ApplicationContextConfigurer;
import io.micronaut.context.annotation.ContextConfigurer;

import java.util.Map;
// end::imports[]

// tag::clazz[]
@ContextConfigurer
public final class RuntimeBootstrapServers implements ApplicationContextConfigurer {
    @Override
    public void configure(ApplicationContextBuilder builder) {
        builder.properties(Map.of(
            "kafka.bootstrap.servers", resolveBootstrapServers()
        ));
    }

    private static String resolveBootstrapServers() {
        return "localhost:9092";
    }
}
// end::clazz[]
