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

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.RxHttpClient
import io.micronaut.runtime.server.EmbeddedServer
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.PollingConditions

@Stepwise
class KafkaUniqueGroupIdSpec extends Specification {

    static final String TOPIC = "groupid-topic"

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    @AutoCleanup
    RxHttpClient httpClient

    def setupSpec() {
        kafkaContainer.start()
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        AbstractKafkaConfiguration.EMBEDDED_TOPICS, [KafkaUniqueGroupIdSpec.TOPIC]
                )
        )
        context = embeddedServer.applicationContext
        httpClient = embeddedServer.applicationContext.createBean(RxHttpClient, embeddedServer.getURL(), new DefaultHttpClientConfiguration(followRedirects: false))
    }

    void "test multiple consumers - single group id"() {
        given:
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        MyClient myClient = context.getBean(MyClient)
        MyConsumer1_1 myConsumer1_1 = context.getBean(MyConsumer1_1)
        MyConsumer1_2 myConsumer1_2 = context.getBean(MyConsumer1_2)

        when:
        myClient.sendMessage("key", "Test message 1")

        then:
        conditions.eventually {
            (myConsumer1_1.lastMessage == 'Test message 1' && myConsumer1_2.lastMessage == null) || (myConsumer1_1.lastMessage == null && myConsumer1_2.lastMessage == 'Test message 1')
            myConsumer1_1.count + myConsumer1_2.count == 1
        }
    }

    void "test multiple consumers - multiple unique group ids - group id defined"() {
        given:
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        MyClient myClient = context.getBean(MyClient)
        MyConsumer2_1 myConsumer2_1 = context.getBean(MyConsumer2_1)
        MyConsumer2_2 myConsumer2_2 = context.getBean(MyConsumer2_2)

        when:
        myClient.sendMessage("key", "Test message 2")

        then:
        conditions.eventually {
            (myConsumer2_1.lastMessage == 'Test message 2' && myConsumer2_2.lastMessage == 'Test message 2')
            myConsumer2_1.count + myConsumer2_2.count == 4 // Twice 'Test message 1' + twice 'Test message 2'
        }
    }

    void "test multiple consumers - multiple unique group ids - group id not defined"() {
        given:
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        MyClient myClient = context.getBean(MyClient)
        MyConsumer3_1 myConsumer3_1 = context.getBean(MyConsumer3_1)
        MyConsumer3_2 myConsumer3_2 = context.getBean(MyConsumer3_2)

        when:
        myClient.sendMessage("key", "Test message 3")

        then:
        conditions.eventually {
            (myConsumer3_1.lastMessage == 'Test message 3' && myConsumer3_2.lastMessage == 'Test message 3')
            myConsumer3_1.count + myConsumer3_2.count == 6 // Twice 3 test messages
        }
    }

    @KafkaClient
    static interface MyClient {
        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void sendMessage(@KafkaKey String key, String message)
    }

    @KafkaListener(groupId = "myGroup", offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer1_1 {
        Integer count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @KafkaListener(groupId = "myGroup", uniqueGroupId = false, offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer1_2 {
        Integer count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @KafkaListener(groupId = "myGroup", uniqueGroupId = true, offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer2_1 {
        Integer count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @KafkaListener(groupId = "myGroup", uniqueGroupId = true, offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer2_2 {
        Integer count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @KafkaListener(/* No group ID defined */ uniqueGroupId = true, offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer3_1 {
        Integer count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

    @KafkaListener(/* No group ID defined */ uniqueGroupId = true, offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer3_2 {
        Integer count = 0
        String lastMessage

        @Topic(KafkaUniqueGroupIdSpec.TOPIC)
        void receiveMessage(@KafkaKey String key, String message) {
            lastMessage = message
            count++
        }
    }

}
