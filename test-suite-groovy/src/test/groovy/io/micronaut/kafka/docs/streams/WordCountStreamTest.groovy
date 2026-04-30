package io.micronaut.kafka.docs.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.testcontainers.kafka.Kafka
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.file.Files
import java.util.UUID

class WordCountStreamTest extends Specification {

    PollingConditions conditions = new PollingConditions()

    void "test word counter"() {
        given:
        def config = new HashMap<>(Kafka.getProperties());
        def uniqueSuffix = UUID.randomUUID().toString()
        def stateDir = Files.createTempDirectory('test-suite-groovy-word-count-stream-')
        stateDir.toFile().deleteOnExit()
        config.put("kafka.enabled", "true");
        config.put("micronaut.application.name", "test-suite-groovy-word-count-stream");
        config.put("kafka.streams.default.application.id", "test-suite-groovy-word-count-stream-${uniqueSuffix}");
        config.put("kafka.streams.default.state.dir", stateDir.toString());
        config.put("spec.name", "WordCountStreamTest");
        config.put("kafka.streams.my-stream.application.id", "test-suite-groovy-my-stream-${uniqueSuffix}");
        config.put("kafka.streams.my-stream.start-kafka-streams", "false");
        config.put("kafka.streams.my-other-stream.application.id", "test-suite-groovy-my-other-stream-${uniqueSuffix}");
        config.put("kafka.streams.my-other-stream.start-kafka-streams", "false");
        ApplicationContext ctx = ApplicationContext.run(config)
        when:
        conditions.within(30) {
            def states = ctx.getBeansOfType(KafkaStreams)*.state()
            states.size() == 3 &&
                states.count { state -> state.isRunningOrRebalancing() } == 1 &&
                states.count { state -> state == State.CREATED } == 2
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
