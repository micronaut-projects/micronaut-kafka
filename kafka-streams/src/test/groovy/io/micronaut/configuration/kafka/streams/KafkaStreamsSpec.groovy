package io.micronaut.configuration.kafka.streams

import io.micronaut.configuration.kafka.streams.listeners.BeforeStartKafkaStreamsListenerImp
import io.micronaut.configuration.kafka.streams.optimization.OptimizationClient
import io.micronaut.configuration.kafka.streams.optimization.OptimizationInteractiveQueryService
import io.micronaut.configuration.kafka.streams.optimization.OptimizationListener
import io.micronaut.configuration.kafka.streams.wordcount.InteractiveQueryServiceExample
import io.micronaut.configuration.kafka.streams.wordcount.WordCountClient
import io.micronaut.configuration.kafka.streams.wordcount.WordCountListener
import io.micronaut.inject.qualifiers.Qualifiers
import org.apache.kafka.streams.KafkaStreams
import spock.lang.IgnoreIf
import spock.lang.Retry

import static io.micronaut.configuration.kafka.streams.optimization.OptimizationStream.OPTIMIZATION_OFF_STORE
import static io.micronaut.configuration.kafka.streams.optimization.OptimizationStream.OPTIMIZATION_ON_STORE
import static io.micronaut.configuration.kafka.streams.wordcount.WordCountStream.WORD_COUNT_STORE

@Retry
class KafkaStreamsSpec extends AbstractTestContainersSpec {

    void "test config"() {
        when:
        def builder = context.getBean(ConfiguredStreamBuilder, Qualifiers.byName('my-stream'))

        then:
        builder.configuration['application.id'] == myStreamApplicationId
        builder.configuration['generic.config'] == "hello"
    }

    void "test config from stream"() {
        when:
        def stream = context.getBean(KafkaStreams, Qualifiers.byName('my-stream'))

        then:
        stream.applicationConfigs.originals()['application.id'] == myStreamApplicationId
        stream.applicationConfigs.originals()['generic.config'] == "hello"
    }

    void "test kafka stream application"() {
        given:
        InteractiveQueryServiceExample interactiveQueryService = context.getBean(InteractiveQueryServiceExample)

        when:
        WordCountClient wordCountClient = context.getBean(WordCountClient)
        wordCountClient.publishSentence("The quick brown fox jumps over the lazy dog. THE QUICK BROWN FOX JUMPED OVER THE LAZY DOG'S BACK")

        WordCountListener countListener = context.getBean(WordCountListener)

        then:
        conditions.eventually {
            countListener.getCount("fox") > 0
            countListener.getCount("jumps") > 0
            interactiveQueryService.getWordCount(WORD_COUNT_STORE, "fox") > 0
            interactiveQueryService.getWordCount(WORD_COUNT_STORE, "jumps") > 0
            interactiveQueryService.<String, Long> getGenericKeyValue(WORD_COUNT_STORE, "the") > 0
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
            optimizationListener.optimizationOnChangelogMessageCount == 0
            //no changelog should be created/used when topology optimization is enabled
            optimizationListener.optimizationOffChangelogMessageCount == 1
            interactiveQueryService.getValue(OPTIMIZATION_OFF_STORE, "key") == "off"
            interactiveQueryService.getValue(OPTIMIZATION_ON_STORE, "key") == "on"
        }
    }

    void "test BeforeStartKafkaStreamsListener execution"() {
        when:
        def builder = context.getBean(BeforeStartKafkaStreamsListenerImp)

        then:
        builder.executed
    }
}
