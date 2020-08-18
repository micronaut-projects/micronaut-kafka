package io.micronaut.configuration.kafka.streams


import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.kafka.streams.KafkaStreams
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class KafkaStreamsDisabledSpec extends Specification {

    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    @Shared
    @AutoCleanup
    ApplicationContext context

    def setupSpec() {
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf(
                        "kafka.enabled", false
                )
        )

        context = embeddedServer.getApplicationContext()
    }

    void "test no streams have been created"() {
        expect:
        context.getBeansOfType(KafkaStreams).isEmpty()
    }

}
