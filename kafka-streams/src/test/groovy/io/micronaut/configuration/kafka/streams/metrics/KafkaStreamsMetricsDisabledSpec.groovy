package io.micronaut.configuration.kafka.streams.metrics

import io.micronaut.configuration.kafka.streams.AbstractTestContainersSpec
import io.micronaut.configuration.kafka.streams.health.KafkaStreamsHealth
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.HttpClient
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup
import spock.lang.Shared

import static io.micronaut.health.HealthStatus.UP

class KafkaStreamsMetricsDisabledSpec extends AbstractTestContainersSpec {

    @Shared @AutoCleanup HttpClient httpClient

    def setupSpec() {
        httpClient = context.createBean(
                HttpClient,
                embeddedServer.getURL(),
                new DefaultHttpClientConfiguration(followRedirects: false)
        )
    }

    def "metrics disabled"() {
        when:
        def bean = context.findBean(KafkaStreamsMetrics)

        then:
        !bean.isPresent()
    }


    void "kafka streams metrics are returned from metrics endpoint"() {
        given:
        def streamsHealth = context.getBean(KafkaStreamsHealth)

        expect:
        conditions.eventually {
            Mono.from(streamsHealth.getResult()).block().status == UP
        }

        and:
        conditions.eventually {
            def response = Mono.from(httpClient.exchange("/metrics", Map)).block()
            Map result = response.body()
            !result.names.contains("kafka-streams.active-buffer-count")
            !result.names.contains("kafka-streams.process-rate")
            !result.names.contains("kafka-streams.alive-stream-threads")
            !result.names.contains("kafka-streams.fetch-rate")
        }
    }

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                ['micrometer.metrics.enabled': true,
                 'endpoints.metrics.sensitive': false,
                 'micronaut.metrics.binders.kafka.streams.enabled': false]
    }
}
