package io.micronaut.configuration.kafka.streams

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.streams.uncaught.OnErrorNoConfigClient
import io.micronaut.configuration.kafka.streams.uncaught.OnErrorNoConfigListener
import io.micronaut.configuration.kafka.streams.uncaught.OnErrorReplaceClient
import io.micronaut.configuration.kafka.streams.uncaught.OnErrorReplaceListener
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.inject.qualifiers.Qualifiers
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

@Slf4j
@Property(name = 'spec.name', value = 'UncaughtExceptionsSpec')
class UncaughtExceptionsSpec extends AbstractTestContainersSpec {

    @Shared
    String onErrorNoConfigAppId = 'kafka-on-error-no-config-' + UUID.randomUUID().toString()

    @Shared
    String onErrorReplaceAppId = 'kafka-on-error-replace-' + UUID.randomUUID().toString()

    @Shared
    String onErrorShutdownAppId = 'kafka-on-error-shutdown-' + UUID.randomUUID().toString()

    @Shared
    String streamsStateDir = "${System.getProperty('java.io.tmpdir')}/uncaught-exceptions-${UUID.randomUUID()}"

    protected Map<String, Object> getConfiguration() {
        return super.getConfiguration() + [
                'kafka.streams.default.state.dir': streamsStateDir,
                'kafka.streams.on-error-no-config.client.id': UUID.randomUUID(),
                'kafka.streams.on-error-no-config.application.id': onErrorNoConfigAppId,
                'kafka.streams.on-error-no-config.group.id': UUID.randomUUID(),
                'kafka.streams.on-error-replace.client.id': UUID.randomUUID(),
                'kafka.streams.on-error-replace.application.id': onErrorReplaceAppId,
                'kafka.streams.on-error-replace.group.id': UUID.randomUUID(),
                'kafka.streams.on-error-replace.uncaught-exception-handler': 'REPLACE_THREAD',
                'kafka.streams.on-error-shutdown.client.id': UUID.randomUUID(),
                'kafka.streams.on-error-shutdown.application.id': onErrorShutdownAppId,
                'kafka.streams.on-error-shutdown.group.id': UUID.randomUUID(),
                'kafka.streams.on-error-shutdown.uncaught-exception-handler': 'SHUTDOWN_APPLICATION']
    }

    void "test uncaught exception with no exception handler"() {
        given: "a stream configured with no exception handler"
        def stream = context.getBean(KafkaStreams, Qualifiers.byName('on-error-no-config'))
        def client = context.getBean(OnErrorNoConfigClient)
        def listener = context.getBean(OnErrorNoConfigListener)

        when: "the stream thread throws an uncaught exception"
        client.send('ERROR')
        client.send('hello')

        then: "the stream enters ERROR state"
        conditions.eventually {
            stream.state() == KafkaStreams.State.ERROR
        }
        stream.metadataForLocalThreads().empty == true
        listener.received == null
    }

    void "test uncaught exception with REPLACE_THREAD"() {
        given: "a stream configured with REPLACE_THREAD"
        def stream = context.getBean(KafkaStreams, Qualifiers.byName("on-error-replace"))
        def client = context.getBean(OnErrorReplaceClient)
        def listener = context.getBean(OnErrorReplaceListener)

        when: "the stream thread throws an uncaught exception"
        client.send('ERROR')
        client.send('hello')

        then: "the stream replaces the thread and keeps running"
        conditions.eventually {
            listener.received == 'HELLO'
        }
        stream.state() == KafkaStreams.State.RUNNING
        stream.metadataForLocalThreads().empty == false
    }

    void "test uncaught exception with SHUTDOWN_APPLICATION stops the app"() {
        given: "a stream configured with SHUTDOWN_APPLICATION"
        def stream = context.getBean(KafkaStreams, Qualifiers.byName("on-error-shutdown"))
        def client = context.getBean(OnErrorShutdownClient)

        when: "the stream thread throws an uncaught exception"
        client.send('ERROR')

        then: "the Micronaut application shuts down"
        conditions.eventually {
            !embeddedServer.isRunning()
        }
        stream.state().hasStartedOrFinishedShuttingDown()
    }

    @Requires(property = "spec.name", value = "UncaughtExceptionsSpec")
    @KafkaClient
    static interface OnErrorShutdownClient {
        @Topic('on-error-shutdown-input')
        void send(String message)
    }
}
