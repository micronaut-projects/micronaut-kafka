/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.streams

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.streams.optimization.OptimizationClient
import io.micronaut.configuration.kafka.streams.optimization.OptimizationInteractiveQueryService

import io.micronaut.configuration.kafka.streams.optimization.OptimizationListener
import io.micronaut.configuration.kafka.streams.optimization.OptimizationStream
import io.micronaut.configuration.kafka.streams.wordcount.InteractiveQueryServiceExample
import io.micronaut.configuration.kafka.streams.wordcount.WordCountClient
import io.micronaut.configuration.kafka.streams.wordcount.WordCountListener
import io.micronaut.configuration.kafka.streams.wordcount.WordCountStream
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.inject.qualifiers.Qualifiers
import org.apache.kafka.streams.KafkaStreams
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

class KafkaStreamsSpec extends Specification {

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared @AutoCleanup ApplicationContext context

    def setupSpec() {
        kafkaContainer.start()
        context = ApplicationContext.run(
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", 'localhost:${random.port}',
                        AbstractKafkaConfiguration.EMBEDDED, true,
                        AbstractKafkaConfiguration.EMBEDDED_TOPICS, [
                        WordCountStream.INPUT,
                        WordCountStream.OUTPUT,
                        WordCountStream.NAMED_WORD_COUNT_INPUT,
                        WordCountStream.NAMED_WORD_COUNT_OUTPUT,
                        OptimizationStream.OPTIMIZATION_ON_INPUT,
                        OptimizationStream.OPTIMIZATION_OFF_INPUT
                ],
                        'kafka.generic.config', "hello",
                        'kafka.streams.my-stream.application.id', 'my-stream',
                        'kafka.streams.my-stream.num.stream.threads', 10,
                        'kafka.streams.optimization-on.application.id', 'optimization-on',
                        'kafka.streams.optimization-on.topology.optimization', 'all',
                        'kafka.streams.optimization-off.application.id', 'optimization-off',
                        'kafka.streams.optimization-off.topology.optimization', 'none'
                )
        )
    }
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
    void "test kafka topology optimization"() {
        given:
        OptimizationInteractiveQueryService interactiveQueryService = context.getBean(OptimizationInteractiveQueryService)
        PollingConditions conditions = new PollingConditions(timeout: 40, delay: 1)

        when:
        OptimizationClient optimizationClient = context.getBean(OptimizationClient)
        optimizationClient.publishOptimizationOffMessage("key", "off")
        optimizationClient.publishOptimizationOnMessage("key", "on")

        OptimizationListener optimizationListener = context.getBean(OptimizationListener)

        then:
        conditions.eventually {
            optimizationListener.getOptimizationOnChangelogMessageCount() == 0 //no changelog should be created/used when topology optimization is enabled
            optimizationListener.getOptimizationOffChangelogMessageCount() == 1
            interactiveQueryService.getValue(OptimizationStream.OPTIMIZATION_OFF_STORE, "key") == "off"
            interactiveQueryService.getValue(OptimizationStream.OPTIMIZATION_ON_STORE, "key") == "on"
        }

    }
}
