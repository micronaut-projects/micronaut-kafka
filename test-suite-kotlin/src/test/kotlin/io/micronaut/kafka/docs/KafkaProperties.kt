package io.micronaut.kafka.docs

import io.micronaut.context.annotation.BootstrapContextCompatible
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.BootstrapPropertySourceLocator
import io.micronaut.context.env.Environment
import io.micronaut.context.env.MapPropertySource
import io.micronaut.context.env.PropertySource
import io.micronaut.testcontainers.kafka.Kafka
import jakarta.inject.Singleton

@Singleton
@BootstrapContextCompatible
@Requires(env = ["kafka"])
class KafkaProperties : BootstrapPropertySourceLocator {

    override fun findPropertySources(environment: Environment): Iterable<PropertySource> {
        return listOf(MapPropertySource("test-property-provider", Kafka.getProperties()))
    }
}
