package io.micronaut.kafka.docs.quickstart

// tag::imports[]
import io.micronaut.context.ApplicationContextBuilder
import io.micronaut.context.ApplicationContextConfigurer
import io.micronaut.context.annotation.ContextConfigurer
// end::imports[]

// tag::clazz[]
@ContextConfigurer
class RuntimeBootstrapServers implements ApplicationContextConfigurer {

    @Override
    void configure(ApplicationContextBuilder builder) {
        builder.properties([
            'kafka.bootstrap.servers': resolveBootstrapServers()
        ])
    }

    private static String resolveBootstrapServers() {
        'localhost:9092'
    }
}
// end::clazz[]
