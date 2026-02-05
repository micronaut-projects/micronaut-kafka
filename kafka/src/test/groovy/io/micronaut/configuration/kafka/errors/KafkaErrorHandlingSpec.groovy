package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import org.apache.kafka.common.TopicPartition
import reactor.core.publisher.Mono

import java.util.concurrent.atomic.AtomicInteger

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class KafkaErrorHandlingSpec extends AbstractEmbeddedServerSpec {

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): ["errors"]]
    }

    void "test an exception that is thrown is not committed"() {
        when:"A consumer throws an exception"
        ErrorClient myClient = context.getBean(ErrorClient)
        myClient.sendMessage("One")
        myClient.sendMessage("Two")

        ErrorCausingConsumer myConsumer = context.getBean(ErrorCausingConsumer)

        then:"The message is re-delivered and eventually handled"
        conditions.eventually {
            myConsumer.received.size() == 2
            myConsumer.count.get() == 3
        }
    }

    void "test custom exception handler in reactive consumer"() {
        when:"A reactive consumer with custom exception handler throws a Mono error"
        ErrorClient myClient = context.getBean(ErrorClient)
        myClient.sendMessage("One")

        ErrorCausingReactiveConsumer myConsumer = context.getBean(ErrorCausingReactiveConsumer)

        then:"The bean's exception handler is used"
        conditions.eventually {
            myConsumer.exceptionHandled
        }
    }

    void "test custom exception handler that throws an exception"() {
        given:"A custom exception handler that throws an exception"
        ErrorCausingCustomExceptionHandlerConsumer myConsumer = context.getBean(ErrorCausingCustomExceptionHandlerConsumer)

        when:"Two messages are produced"
        ErrorClient myClient = context.getBean(ErrorClient)
        myClient.sendMessage('ERROR') // This one will create the problem
        myClient.sendMessage('OK')    // This one should be received normally

        then:"The error message is received"
        conditions.eventually {
            myConsumer.messagesReceived.contains("ERROR")
        }

        and:"The custom exception handler receives the kafka listener exception"
        conditions.eventually {
            myConsumer.exceptionsReceived.any { it.message == "Consumer problem: ERROR" }
        }

        and:"The next message is received normally"
        conditions.eventually {
            myConsumer.messagesReceived.contains("OK")
        }

        and:"The custom exception handler does NOT receive its own exception"
        conditions.eventually {
            !myConsumer.exceptionsReceived.any { it.message == "Custom exception handler problem" }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorHandlingSpec')
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = SYNC)
    static class ErrorCausingConsumer implements KafkaListenerExceptionHandler {
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []

        @Topic("errors")
        void handleMessage(String message) {
            if (count.getAndIncrement() == 1) {
                throw new RuntimeException("Won't handle first")
            }
            received << message
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

    @Requires(property = 'spec.name', value = 'KafkaErrorHandlingSpec')
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = SYNC)
    static class ErrorCausingReactiveConsumer implements KafkaListenerExceptionHandler {
        boolean exceptionHandled = false

        @Topic("errors")
        Mono<Void> handleMessage(String message) {
            return Mono.error(new RuntimeException())
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptionHandled = true
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorHandlingSpec')
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = SYNC, uniqueGroupId = true, properties = @Property(name = "max.poll.records", value = "1"))
    static class ErrorCausingCustomExceptionHandlerConsumer implements KafkaListenerExceptionHandler {

        List<String> messagesReceived = []
        List<KafkaListenerException> exceptionsReceived = []

        @Topic("errors")
        void receive(String message) {
            messagesReceived << message
            if (message == 'ERROR') {
                throw new RuntimeException("Consumer problem: $message")
            }
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptionsReceived << exception
            throw new RuntimeException("Custom exception handler problem")
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorHandlingSpec')
    @KafkaClient
    static interface ErrorClient {
        @Topic("errors")
        void sendMessage(String message)
    }
}
