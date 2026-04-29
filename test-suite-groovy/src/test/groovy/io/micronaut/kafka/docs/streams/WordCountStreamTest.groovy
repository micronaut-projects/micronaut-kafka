package io.micronaut.kafka.docs.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.testcontainers.kafka.Kafka
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

class WordCountStreamTest extends Specification {

    PollingConditions conditions = new PollingConditions()

    void "test word counter"() {
        given:
        def config = new HashMap<>(Kafka.getProperties());
        config.put("kafka.enabled", "true");
        config.put("spec.name", "WordCountStreamTest");
        config.put("kafka.streams.my-stream.application.id", "test-suite-groovy-my-stream");
        config.put("kafka.streams.my-stream.start-kafka-streams", "false");
        config.put("kafka.streams.my-other-stream.application.id", "test-suite-groovy-my-other-stream");
        config.put("kafka.streams.my-other-stream.start-kafka-streams", "false");
        ApplicationContext ctx = ApplicationContext.run(config)
        when:
        conditions.within(30) {
            def states = ctx.getBeansOfType(KafkaStreams)*.state()
            states.any { state -> state.isRunningOrRebalancing() } &&
                states.findAll { state -> state != State.CREATED }.every { state -> state.isRunningOrRebalancing() }
        }
        WordCountClient client = ctx.getBean(WordCountClient)
        client.publishSentence('test to test for words')

        then:
        WordCountListener listener = ctx.getBean(WordCountListener)
        conditions.within(30) {
            listener.getWordCounts().size() == 4 &&
            listener.getCount('test')  == 2 &&
            listener.getCount('to')    == 1 &&
            listener.getCount('for')   == 1 &&
            listener.getCount('words') == 1
        }

        cleanup:
        ctx.close()
    }
}
