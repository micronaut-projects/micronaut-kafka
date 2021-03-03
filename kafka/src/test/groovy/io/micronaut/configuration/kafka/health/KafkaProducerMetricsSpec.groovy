package io.micronaut.configuration.kafka.health

import groovy.transform.EqualsAndHashCode
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.metrics.management.endpoint.MetricsEndpoint
import io.micronaut.context.annotation.Requires
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.RxHttpClient
import io.micronaut.messaging.annotation.Header
import io.reactivex.Single
import org.apache.kafka.clients.producer.RecordMetadata
import spock.lang.AutoCleanup
import spock.lang.Shared

import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class KafkaProducerMetricsSpec extends AbstractEmbeddedServerSpec {

    @Shared @AutoCleanup RxHttpClient httpClient

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                ['micrometer.metrics.enabled' : true,
                 'endpoints.metrics.sensitive': false,
                 (EMBEDDED_TOPICS)            : ['words', 'books', 'words-records', 'books-records']]
    }

    def setupSpec() {
        httpClient = context.createBean(
                RxHttpClient,
                embeddedServer.getURL(),
                new DefaultHttpClientConfiguration(followRedirects: false)
        )
    }

    void "test simple producer"() {
        given:
        context.containsBean(MeterRegistry)
        context.containsBean(MetricsEndpoint)
        context.containsBean(MyClientMetrics)
        context.containsBean(KafkaHealthIndicator)

        when:
        context.getBean(MyClientMetrics).sendGetRecordMetadata("key", "value")

        then:
        conditions.eventually {
            def response = httpClient.exchange("/metrics", Map).blockingFirst()
            Map result = response.body()
            !result.names.contains("kafka.consumer.record-error-rate")
            result.names.contains("kafka.producer.count")
            result.names.contains("kafka.producer.record-error-rate")
            !result.names.contains("kafka.producer.bytes-consumed-total")
            !result.names.contains("kafka.count")
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerMetricsSpec')
    @KafkaClient
    static interface MyClientMetrics {
        @Topic("words-metrics")
        void sendSentence(@KafkaKey String key, String sentence, @Header String topic)

        @Topic("words-metrics-two")
        RecordMetadata sendGetRecordMetadata(@KafkaKey String key, String sentence)

        @Topic("books-metrics")
        Single<Book> sendReactive(@KafkaKey String key, Book book)
    }

    @EqualsAndHashCode
    static class Book {
        String title
    }
}
