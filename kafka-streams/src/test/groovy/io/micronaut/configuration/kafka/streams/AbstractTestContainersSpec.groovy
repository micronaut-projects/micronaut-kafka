package io.micronaut.configuration.kafka.streams


import io.micronaut.configuration.kafka.streams.optimization.OptimizationStream
import io.micronaut.configuration.kafka.streams.wordcount.WordCountStream
import spock.lang.Shared

abstract class AbstractTestContainersSpec extends AbstractEmbeddedServerSpec {

    @Shared
    String myStreamApplicationId = 'my-stream-' + UUID.randomUUID().toString()

    @Shared
    String optimizationOnApplicationId = 'optimization-on-' + UUID.randomUUID().toString()

    @Shared
    String optimizationOffApplicationId = 'optimization-off-' + UUID.randomUUID().toString()

    protected Map<String, Object> getConfiguration() {
        super.getConfiguration() + ['kafka.generic.config': "hello",
                                    'kafka.streams.my-stream.application.id': myStreamApplicationId,
                                    'kafka.streams.my-stream.num.stream.threads': 10,
                                    'kafka.streams.optimization-on.application.id': optimizationOnApplicationId,
                                    'kafka.streams.optimization-on.topology.optimization': 'all',
                                    'kafka.streams.optimization-off.application.id': optimizationOffApplicationId,
                                    'kafka.streams.optimization-off.topology.optimization': 'none']
    }

    @Override
    void afterKafkaStarted() {
        [
                WordCountStream.INPUT,
                WordCountStream.OUTPUT,
                WordCountStream.NAMED_WORD_COUNT_INPUT,
                WordCountStream.NAMED_WORD_COUNT_OUTPUT,
                OptimizationStream.OPTIMIZATION_ON_INPUT,
                OptimizationStream.OPTIMIZATION_OFF_INPUT
        ].forEach(topic -> {
            createTopic(topic.toString(), 1, 1)
        })
    }

}
