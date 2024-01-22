package io.micronaut.configuration.kafka.streams

import java.time.Duration

class CloseTimeoutKafkaStreamsSpec extends AbstractTestContainersSpec {

    void "test default close configuration"() {
        when:
        def factory = context.getBean(KafkaStreamsFactory)
        def stream = factory.getStreams().entrySet().stream().filter(e -> e.getValue().name == 'my-stream').findAny().orElseThrow()

        then:
        stream.value.getCloseTimeout() == Duration.ofMillis(1)
        stream.key.state().isRunningOrRebalancing()

        when:
        factory.close()

        then:
        stream.key.state().hasStartedOrFinishedShuttingDown()
    }

    @Override
    Map<String, Object> getConfiguration() {
        return super.getConfiguration() + ["kafka.streams.my-stream.close-timeout": '1ms']
    }
}
