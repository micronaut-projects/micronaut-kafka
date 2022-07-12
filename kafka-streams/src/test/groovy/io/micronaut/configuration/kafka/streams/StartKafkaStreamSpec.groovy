package io.micronaut.configuration.kafka.streams

import io.micronaut.configuration.kafka.streams.startkafkastreams.StartKafkaStreamsOff
import io.micronaut.inject.qualifiers.Qualifiers
import org.apache.kafka.streams.KafkaStreams.State
import spock.lang.Retry

@Retry
class StartKafkaStreamSpec extends AbstractTestContainersSpec {

    void "should not start kafkaStreams"() {
        when:
        def builder = context.getBean(ConfiguredStreamBuilder, Qualifiers.byName(StartKafkaStreamsOff.START_KAFKA_STREAMS_OFF))
        def kafkaStreamsFactory = context.getBean(KafkaStreamsFactory)

        then:
        for (entry in kafkaStreamsFactory.streams) {
            if (entry.value == builder) {
                assert entry.key.state() == State.CREATED
            }
        }
    }

    @Override
    protected Map<String, Object> getConfiguration() {
        super.getConfiguration() + ["kafka.streams.start-kafka-streams-off.start-kafka-streams": 'false']
    }
}
