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
package io.micronaut.configuration.kafka.annotation

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.messaging.exceptions.MessagingClientException
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

class KafkaClientSpec extends Specification {

    void "test send message when Kafka is not available"() {
        given:
        ApplicationContext ctx = ApplicationContext.run()
        MyClient client = ctx.getBean(MyClient)

        when:
        client.sendSync("test", "hello-world", [new RecordHeader("hello", "world".bytes)])

        then:
        def e = thrown(MessagingClientException)

        cleanup:
        ctx.close()
    }

    void "test reactive send message when Kafka is not available"() {
        given:
        ApplicationContext ctx = ApplicationContext.run()
        MyClient client = ctx.getBean(MyClient)

        when:
        client.sendRx("test", "hello-world", new RecordHeaders([new RecordHeader("hello", "world".bytes)])).block()

        then:
        def e = thrown(MessagingClientException)

        cleanup:
        ctx.close()

    }

    void "test future send message when Kafka is not available"() {
        given:
        ApplicationContext ctx = ApplicationContext.run()
        MyClient client = ctx.getBean(MyClient)

        when:
        client.sendSentence("test", "hello-world").get(1, TimeUnit.SECONDS)

        then:
        def e = thrown(ExecutionException)
        e.cause instanceof MessagingClientException

        cleanup:
        ctx.close()
    }

    @KafkaClient(maxBlock  = '1s', acks = KafkaClient.Acknowledge.ALL)
    static interface MyClient {
        @Topic("words")
        CompletableFuture<String> sendSentence(@KafkaKey String key, String sentence)

        @Topic("words")
        @KafkaClient(
                properties = [
                        @Property(name = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                  value = "org.apache.kafka.common.serialization.ByteArraySerializer"),
                        @Property(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                value = "org.apache.kafka.common.serialization.ByteArraySerializer")
                ]
        )
        String sendSync(@KafkaKey String key, String sentence, Collection<Header> headers)

        @Topic("words")
        Mono<String> sendRx(@KafkaKey String key, String sentence, Headers headers)
    }
}
