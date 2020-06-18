package io.micronaut.configuration.kafka.streams

import io.micronaut.configuration.kafka.streams.listeners.BeforeStartKafkaStreamsListenerImp
import io.micronaut.configuration.kafka.streams.optimization.OptimizationClient
import io.micronaut.configuration.kafka.streams.optimization.OptimizationInteractiveQueryService
import io.micronaut.configuration.kafka.streams.optimization.OptimizationListener
import io.micronaut.configuration.kafka.streams.optimization.OptimizationStream
import io.micronaut.configuration.kafka.streams.wordcount.InteractiveQueryServiceExample
import io.micronaut.configuration.kafka.streams.wordcount.WordCountClient
import io.micronaut.configuration.kafka.streams.wordcount.WordCountListener
import io.micronaut.configuration.kafka.streams.wordcount.WordCountStream
import io.micronaut.inject.qualifiers.Qualifiers
import org.apache.kafka.streams.KafkaStreams
import spock.lang.IgnoreIf
import spock.lang.Retry
import spock.util.concurrent.PollingConditions

@Retry
class KafkaStreamsSpec extends AbstractTestContainersSpec {

    void "test config"() {
        when:
        def builder = context.getBean(ConfiguredStreamBuilder, Qualifiers.byName('my-stream'))

        then:
        builder.configuration['application.id'] == "my-stream"
        builder.configuration['generic.config'] == "hello"
    }

    void "test config from stream"() {
        when:
        def stream = context.getBean(KafkaStreams, Qualifiers.byName('my-stream'))

        then:
        stream.config.originals().get('application.id') == "my-stream"
        stream.config.originals().get('generic.config') == "hello"
    }

    void "test kafka stream application"() {
        given:
        InteractiveQueryServiceExample interactiveQueryService = context.getBean(InteractiveQueryServiceExample)
        PollingConditions conditions = new PollingConditions(timeout: 40, delay: 1)

        when:
        WordCountClient wordCountClient = context.getBean(WordCountClient)
        wordCountClient.publishSentence("The quick brown fox jumps over the lazy dog. THE QUICK BROWN FOX JUMPED OVER THE LAZY DOG'S BACK")

        WordCountListener countListener = context.getBean(WordCountListener)

        then:
        conditions.eventually {
            countListener.getCount("fox") > 0
            countListener.getCount("jumps") > 0
            interactiveQueryService.getWordCount(WordCountStream.WORD_COUNT_STORE, "fox") > 0
            interactiveQueryService.getWordCount(WordCountStream.WORD_COUNT_STORE, "jumps") > 0
            interactiveQueryService.<String, Long> getGenericKeyValue(WordCountStream.WORD_COUNT_STORE, "the") > 0

            println countListener.wordCounts
            println interactiveQueryService.getWordCount(WordCountStream.WORD_COUNT_STORE, "fox")
            println interactiveQueryService.getWordCount(WordCountStream.WORD_COUNT_STORE, "jumps")
            println interactiveQueryService.<String, Long> getGenericKeyValue(WordCountStream.WORD_COUNT_STORE, "the")
        }

    }

    /**
     * This unit test utilized the fact that
     * KTables that are constructed directly from a topic
     * can be optimized to not need an internal changelog topic.
     * Instead, Kafka Streams can restore the state of the KTable
     * via the original source topic.
     *
     * This unit test was designed to fix issue #65.
     *
     * @author jgray1206
     */
    @IgnoreIf({System.getenv("GITHUB_WORKFLOW")})
    void "test kafka topology optimization"() {
        given:
        OptimizationInteractiveQueryService interactiveQueryService =
                context.getBean(OptimizationInteractiveQueryService)
        PollingConditions conditions = new PollingConditions(timeout: 40, delay: 1)


        when:

        OptimizationListener optimizationListener = context.getBean(OptimizationListener)

        then:
        conditions.eventually {
            optimizationListener.ready
        }

        OptimizationClient optimizationClient = context.getBean(OptimizationClient)

        optimizationClient.publishOptimizationOffMessage("key", "off")
        optimizationClient.publishOptimizationOnMessage("key", "on")


        then:
        conditions.eventually {
            optimizationListener.getOptimizationOnChangelogMessageCount() == 0
            //no changelog should be created/used when topology optimization is enabled
            optimizationListener.getOptimizationOffChangelogMessageCount() == 1
            interactiveQueryService.getValue(OptimizationStream.OPTIMIZATION_OFF_STORE, "key") == "off"
            interactiveQueryService.getValue(OptimizationStream.OPTIMIZATION_ON_STORE, "key") == "on"
        }

    }

    void "test BeforeStartKafkaStreamsListener execution"() {
        when:
        def builder = context.getBean(BeforeStartKafkaStreamsListenerImp)
        then:
        builder.executed
    }
}
