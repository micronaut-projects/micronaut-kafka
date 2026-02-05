package io.micronaut.configuration.kafka.health

import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.metrics.ConsumerKafkaMetricsReporter
import io.micronaut.configuration.metrics.management.endpoint.MetricsEndpoint
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpResponse
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.HttpClient
import io.micronaut.messaging.annotation.MessageHeader
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Retry

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

@Retry
class KafkaConsumerMetricsSpec extends AbstractEmbeddedServerSpec {

    @Shared @AutoCleanup HttpClient httpClient
    @Shared MeterRegistry meterRegistry

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                ["micrometer.metrics.enabled" : true,
                 'endpoints.metrics.sensitive': false,
                 (EMBEDDED_TOPICS)            : ["words-metrics", "words", "books", "words-records", "books-records"]]
    }

    void setupSpec() {
        httpClient = context.createBean(
                HttpClient,
                embeddedServer.URL,
                new DefaultHttpClientConfiguration(followRedirects: false)
        )
        meterRegistry = context.getBean(MeterRegistry)
    }

    void "test simple consumer"() {
        given:
        context.containsBean(MeterRegistry)
        context.containsBean(MetricsEndpoint)
        context.containsBean(MyConsumerMetrics)
        context.containsBean(KafkaHealthIndicator)

        expect:
        conditions.eventually {
            HttpResponse<Map> response = Mono.from(httpClient.exchange("/metrics", Map)).block()
            Map result = response.body()
            result.names.contains("kafka.consumer.bytes-consumed-total")
            !result.names.contains("kafka.consumer.record-error-rate")
            !result.names.contains("kafka.producer.count")
            !result.names.contains("kafka.producer.record-error-rate")
            !result.names.contains("kafka.producer.bytes-consumed-total")
            !result.names.contains("kafka.count")

            def preferredReadReplica = Mono.from(httpClient.exchange("/metrics/kafka.consumer.records-lead-avg", Map)).block()
            Map metricBody = preferredReadReplica.body()
            metricBody.availableTags.size() == 3
            metricBody.availableTags*.tag == [ConsumerKafkaMetricsReporter.PARTITION_TAG, ConsumerKafkaMetricsReporter.TOPIC_TAG, ConsumerKafkaMetricsReporter.CLIENT_ID_TAG]

            def requestRate = Mono.from(httpClient.exchange("/metrics/kafka.consumer.request-rate", Map)).block()
            Map requestRateBody = requestRate.body()
            requestRateBody.availableTags.size() == 2
            requestRateBody.availableTags*.tag == [ConsumerKafkaMetricsReporter.NODE_ID_TAG, ConsumerKafkaMetricsReporter.CLIENT_ID_TAG]
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaConsumerMetricsSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class MyConsumerMetrics {
        int wordCount
        String lastTopic

        @Topic("words-metrics")
        void countWord(String sentence, @MessageHeader String topic) {
            wordCount += sentence.split(/\s/).size()
            lastTopic = topic
        }
    }
}
