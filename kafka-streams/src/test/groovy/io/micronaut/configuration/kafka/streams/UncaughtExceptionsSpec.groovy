package io.micronaut.configuration.kafka.streams

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.streams.uncaught.OnErrorNoConfigClient
import io.micronaut.configuration.kafka.streams.uncaught.OnErrorNoConfigListener
import io.micronaut.configuration.kafka.streams.uncaught.OnErrorReplaceClient
import io.micronaut.configuration.kafka.streams.uncaught.OnErrorReplaceListener
import io.micronaut.context.annotation.Property
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

    protected Map<String, Object> getConfiguration() {
        return super.getConfiguration() + [
                'kafka.streams.on-error-no-config.client.id': UUID.randomUUID(),
                'kafka.streams.on-error-no-config.application.id': onErrorNoConfigAppId,
                'kafka.streams.on-error-no-config.group.id': UUID.randomUUID(),
                'kafka.streams.on-error-replace.client.id': UUID.randomUUID(),
                'kafka.streams.on-error-replace.application.id': onErrorReplaceAppId,
                'kafka.streams.on-error-replace.group.id': UUID.randomUUID(),
                'kafka.streams.on-error-replace.uncaught-exception-handler': 'REPLACE_THREAD']
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
}
