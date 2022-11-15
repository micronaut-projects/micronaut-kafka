package io.micronaut.configuration.kafka.streams.metrics

import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.kafka.streams.AbstractTestContainersSpec
import io.micronaut.configuration.kafka.streams.health.KafkaStreamsHealth
import io.micronaut.configuration.kafka.streams.wordcount.WordCountClient
import io.micronaut.configuration.kafka.streams.wordcount.WordCountListener
import io.micronaut.configuration.metrics.management.endpoint.MetricsEndpoint
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.HttpClient
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup
import spock.lang.Shared

import static io.micronaut.health.HealthStatus.UP

class KafkaStreamsMetricsSpec extends AbstractTestContainersSpec {

    @Shared @AutoCleanup HttpClient httpClient

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                ['micrometer.metrics.enabled': true,
                 'endpoints.metrics.sensitive': false]
    }

    def setupSpec() {
        httpClient = context.createBean(
                HttpClient,
                embeddedServer.getURL(),
                new DefaultHttpClientConfiguration(followRedirects: false)
        )
    }

    void "kafka streams metrics are returned from metrics endpoint"() {
        given:
        context.containsBean(MeterRegistry)
        context.containsBean(MetricsEndpoint)
        def streamsHealth = context.getBean(KafkaStreamsHealth)

        expect:
        conditions.eventually {
            Mono.from(streamsHealth.getResult()).block().status == UP
        }

        and:
        conditions.eventually {
            def response = Mono.from(httpClient.exchange("/metrics", Map)).block()
            Map result = response.body()
            result.names.contains("kafka-streams.active-buffer-count")
            result.names.contains("kafka-streams.process-rate")
            result.names.contains("kafka-streams.alive-stream-threads")
            result.names.contains("kafka-streams.fetch-rate")
        }
    }

    void "kafka streams metrics change from 0"() {
        given:
        context.containsBean(MeterRegistry)
        context.containsBean(MetricsEndpoint)
        def streamsHealth = context.getBean(KafkaStreamsHealth)
        conditions.eventually {
            Mono.from(streamsHealth.getResult()).block().status == UP
        }

        when:
        WordCountClient wordCountClient = context.getBean(WordCountClient)
        wordCountClient.publishSentence("The quick brown fox jumps over the lazy dog. THE QUICK BROWN FOX JUMPED OVER THE LAZY DOG'S BACK")

        WordCountListener countListener = context.getBean(WordCountListener)

        then:
        conditions.eventually {
            countListener.getCount("fox") > 0

            def response = Mono.from(httpClient.exchange("/metrics/kafka-streams.records-consumed-total", Map)).block()
            Map result = response.body()
            result.measurements[0].statistic == 'COUNT'
            result.measurements[0].value > 0
        }
        conditions.eventually {
            def response = Mono.from(httpClient.exchange("/metrics/kafka-streams.alive-stream-threads", Map)).block()
            Map result = response.body()
            result.measurements[0].statistic  == "VALUE"
            result.measurements[0].value > 0.0
        }
    }

}
