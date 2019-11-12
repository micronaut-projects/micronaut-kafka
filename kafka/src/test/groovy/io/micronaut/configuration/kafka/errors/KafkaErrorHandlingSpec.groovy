package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.messaging.MessageHeaders
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.atomic.AtomicInteger

class KafkaErrorHandlingSpec extends Specification {
    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    def setupSpec() {
        kafkaContainer.start()
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        AbstractKafkaConfiguration.EMBEDDED_TOPICS, ["errors"]
                )
        )
    }

    void "test an exception that is thrown is not committed"() {
        when:"A consumer throws an exception"
        def context = embeddedServer.getApplicationContext()
        ErrorClient myClient = context.getBean(ErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")

        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        ErrorCausingConsumer myConsumer = context.getBean(ErrorCausingConsumer)

        then:"The message is re-delivered and eventually handled"
        conditions.eventually {
            myConsumer.received.size() == 2
            myConsumer.count.get() == 3
        }


    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST, offsetStrategy = OffsetStrategy.SYNC)
    static class ErrorCausingConsumer implements KafkaListenerExceptionHandler {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []

        @Topic("errors")
        void handleMessage(String message) {
            if (count.getAndIncrement() == 1) {
                throw new RuntimeException("Won't handle first")
            }
            received.add(message)
        }

        @Override
        void handle(KafkaListenerException exception) {
            def record = exception.consumerRecord.orElse(null)
            def consumer = exception.kafkaConsumer
            consumer.seek(
                    new TopicPartition("errors", record.partition()),
                    record.offset()
            )
        }
    }

    @KafkaClient
    static interface ErrorClient {
        @Topic("errors")
        void sendMessage(String message)
    }
}
