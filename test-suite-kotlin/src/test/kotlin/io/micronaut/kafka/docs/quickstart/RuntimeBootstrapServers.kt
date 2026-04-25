package io.micronaut.kafka.docs.quickstart

// tag::imports[]
import io.micronaut.context.ApplicationContextBuilder
import io.micronaut.context.ApplicationContextConfigurer
import io.micronaut.context.annotation.ContextConfigurer
// end::imports[]

// tag::clazz[]
@ContextConfigurer
class RuntimeBootstrapServers : ApplicationContextConfigurer {
    override fun configure(builder: ApplicationContextBuilder) {
        builder.properties(
            mapOf(
                "kafka.bootstrap.servers" to resolveBootstrapServers()
            )
        )
    }
// end::clazz[]

    private fun resolveBootstrapServers(): String = "localhost:9092"
}
