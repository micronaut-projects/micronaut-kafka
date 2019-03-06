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
package io.micronaut.configuration.kafka.health

import groovy.transform.EqualsAndHashCode
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.metrics.management.endpoint.MetricsEndpoint
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.RxHttpClient
import io.micronaut.messaging.annotation.Header
import io.micronaut.runtime.server.EmbeddedServer
import io.reactivex.Single
import org.apache.kafka.clients.producer.RecordMetadata
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class KafkaProducerMetricsSpec extends Specification {

    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer,
            CollectionUtils.mapOf(
                    "kafka.bootstrap.servers", 'localhost:${random.port}',
                    "micrometer.metrics.enabled", true,
                    'endpoints.metrics.sensitive', false,
                    AbstractKafkaConfiguration.EMBEDDED, true,
                    AbstractKafkaConfiguration.EMBEDDED_TOPICS, ["words", "books", "words-records", "books-records"]
            )
    )

    @Shared
    @AutoCleanup
    ApplicationContext context = embeddedServer.applicationContext

    @Shared
    @AutoCleanup
    RxHttpClient httpClient = embeddedServer.applicationContext.createBean(RxHttpClient, embeddedServer.getURL(), new DefaultHttpClientConfiguration(followRedirects: false))

    void "test simple consumer"() {
        expect:
        context.containsBean(MeterRegistry)
        context.containsBean(MetricsEndpoint)
        context.containsBean(MyClientMetrics)

        when:
        def response = httpClient.exchange("/metrics", Map).blockingFirst()
        Map result = response.body()

        then:
        result.names.contains("kafka.count")
    }

    @KafkaClient
    static interface MyClientMetrics {
        @Topic("words-metrics")
        void sendSentence(@KafkaKey String key, String sentence, @Header String topic)

        @Topic("words-metrics")
        RecordMetadata sendGetRecordMetadata(@KafkaKey String key, String sentence)

        @Topic("books-metrics")
        Single<Book> sendReactive(@KafkaKey String key, Book book)
    }

    @EqualsAndHashCode
    static class Book {
        String title
    }
}
