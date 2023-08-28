package io.micronaut.configuration.kafka.streams

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.streams.uncaught.CustomUncaughtHandlerStreamFactory
import io.micronaut.configuration.kafka.streams.uncaught.MyStreamsUncaughtExceptionHandler
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.inject.qualifiers.Qualifiers
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST

@Property(name = 'spec.name', value = 'CustomUncaughtHandlerSpec')
class CustomUncaughtHandlerSpec extends AbstractTestContainersSpec {

    protected Map<String, Object> getConfiguration() {
        return super.getConfiguration() + [
                'kafka.streams.my-custom-handler.client.id'                 : UUID.randomUUID(),
                'kafka.streams.my-custom-handler.application.id'            : 'my-custom-handler-' + UUID.randomUUID().toString(),
                'kafka.streams.my-custom-handler.group.id'                  : UUID.randomUUID()
        ]
    }

    void "test uncaught exception with custom handler"() {
        given: "a stream configured with custom handler"
        def stream = context.getBean(KafkaStreams, Qualifiers.byName(CustomUncaughtHandlerStreamFactory.CUSTOM_HANDLER))
        def client = context.getBean(MyCustomHandlerClient)
        def listener = context.getBean(MyCustomHandlerListener)

        when: "the stream thread throws an uncaught exception"
        client.send('ERROR')
        client.send('hello')

        then: "the stream replaces the thread and keeps running"
        conditions.eventually {
            listener.received == 'HELLO'
        }
        stream.state() == KafkaStreams.State.RUNNING
        stream.metadataForLocalThreads().empty == false

        and: "the custom uncaught exception handler avoided the danger"
        context.getBean(MyStreamsUncaughtExceptionHandler).dangerAvoided
    }

    @Requires(property = "spec.name", value = "CustomUncaughtHandlerSpec")
    @KafkaClient
    static interface MyCustomHandlerClient {
        @Topic(CustomUncaughtHandlerStreamFactory.CUSTOM_HANDLER_INPUT)
        void send(String message)
    }

    @Requires(property = "spec.name", value = "CustomUncaughtHandlerSpec")
    @KafkaListener(offsetReset = EARLIEST, groupId = "MyCustomHandlerListener", uniqueGroupId = true)
    static class MyCustomHandlerListener {
        String received
        @Topic(CustomUncaughtHandlerStreamFactory.CUSTOM_HANDLER_OUTPUT)
        void receive(String message) {
            received = message
        }
    }
}
