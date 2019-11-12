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
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import org.apache.kafka.common.errors.SerializationException
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.PollingConditions

import javax.inject.Inject
import java.util.concurrent.ConcurrentHashMap

@Stepwise
class KafkaTypeConversionSpec extends Specification {

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared
    @AutoCleanup
    ApplicationContext context

    def setupSpec() {
        kafkaContainer.start()
        context = ApplicationContext.run(
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        AbstractKafkaConfiguration.EMBEDDED_TOPICS, ["uuids"]
                )
        )
    }

    void "test send valid UUID key"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        def uuid = UUID.randomUUID()
        myClient.send(uuid.toString(), "test")

        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        MyListener myConsumer = context.getBean(MyListener)

        then:
        conditions.eventually {
            myConsumer.messages[uuid] == 'test'
        }
    }

    void "test send invalid UUID key"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        def uuid = "bunch 'o junk"
        myClient.send(uuid, "test")

        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        MyListener myConsumer = context.getBean(MyListener)

        then:
        conditions.eventually {
            myConsumer.lastException != null
            myConsumer.lastException.cause instanceof SerializationException
            myConsumer.lastException.cause.printStackTrace()
            myConsumer.lastException.cause.message.contains("deserializing key/value for partition")
        }
    }

    @KafkaListener(groupId = "MyUuidGroup", offsetReset = OffsetReset.EARLIEST)
    static class MyListener implements KafkaListenerExceptionHandler {

        Map<UUID, String> messages = new ConcurrentHashMap<>()
        KafkaListenerException lastException

        @Inject
        KafkaListenerExceptionHandler defaultExceptionHandler

        @Topic("uuids")
        void receive(@KafkaKey UUID key, String message) {
            messages.put(key, message)
        }

        @Override
        void handle(KafkaListenerException exception) {
            lastException = exception
            defaultExceptionHandler.handle(exception)
        }
    }

    @KafkaClient
    static interface MyClient {
        @Topic("uuids")
        void send(@KafkaKey String key, String message)
    }
}
