package io.micronaut.configuration.kafka.streams

import io.micronaut.configuration.kafka.streams.wordcount.WordCountStream
import io.micronaut.inject.qualifiers.Qualifiers
import org.apache.kafka.streams.KafkaStreams.State
import spock.lang.Retry

@Retry
class KafkaStreamsFactorySpec extends AbstractTestContainersSpec {

    void "should not start kafkaStreams"() {
        given:
        def builder = context.getBean(ConfiguredStreamBuilder, Qualifiers.byName(WordCountStream.START_KAFKA_STREAMS_OFF))

        def kafkaStreamsFactory = context.getBean(KafkaStreamsFactory)


        when:
        builder.build()

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
