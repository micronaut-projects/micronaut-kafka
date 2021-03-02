package io.micronaut.configuration.kafka.health

import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.metrics.management.endpoint.MetricsEndpoint
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.RxHttpClient
import io.micronaut.messaging.annotation.Header
import io.micronaut.runtime.server.EmbeddedServer
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class KafkaConsumerMetricsSpec extends Specification {

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    MeterRegistry meterRegistry

    @Shared
    @AutoCleanup
    RxHttpClient httpClient

    def setupSpec() {
        kafkaContainer.start()
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        "micrometer.metrics.enabled", true,
                        'endpoints.metrics.sensitive', false,
                        EMBEDDED_TOPICS, ["words-metrics", "words", "books", "words-records", "books-records"]
                )
        )
        context = embeddedServer.applicationContext
        httpClient = embeddedServer.applicationContext.createBean(
                RxHttpClient,
                embeddedServer.getURL(),
                new DefaultHttpClientConfiguration(followRedirects: false))
        meterRegistry = context.getBean(MeterRegistry)
    }

    void "test simple consumer"() {
        given:
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        context.containsBean(MeterRegistry)
        context.containsBean(MetricsEndpoint)
        context.containsBean(MyConsumerMetrics)
        context.containsBean(KafkaHealthIndicator)

        expect:
        conditions.eventually {
            def response = httpClient.exchange("/metrics", Map).blockingFirst()
            Map result = response.body()
            result.names.contains("kafka.consumer.bytes-consumed-total")
            !result.names.contains("kafka.consumer.record-error-rate")
            !result.names.contains("kafka.producer.count")
            !result.names.contains("kafka.producer.record-error-rate")
            !result.names.contains("kafka.producer.bytes-consumed-total")
            !result.names.contains("kafka.count")
        }
    }

    @KafkaListener(offsetReset = EARLIEST)
    static class MyConsumerMetrics {
        int wordCount
        String lastTopic

        @Topic("words-metrics")
        void countWord(String sentence, @Header String topic) {
            wordCount += sentence.split(/\s/).size()
            lastTopic = topic
        }
    }
}
