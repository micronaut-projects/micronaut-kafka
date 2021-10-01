package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.IntStream

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG

class KafkaErrorHandlingSpec extends AbstractEmbeddedServerSpec {

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): ["errors"], ('kafka.consumers.default.' + MAX_POLL_RECORDS_CONFIG): "10"]
    }

    void "test an exception that is thrown is not committed"() {
        when:"A consumer throws an exception"
        ErrorClient myClient = context.getBean(ErrorClient)
        IntStream.range(0, 30).forEach(i -> myClient.sendMessage(String.valueOf(i)))
        ErrorCausingConsumer myConsumer = context.getBean(ErrorCausingConsumer)
        KafkaConsumer<byte[], String> consumer = createKafkaConsumer()

        then:"The message is re-delivered and eventually handled"
        conditions.eventually {
            myConsumer.received.size() == 30
            myConsumer.count.get() == 31
        }


        TopicPartition topicPartition = new TopicPartition("errors", 0)
        conditions.eventually {
            consumer.committed(Set.of(topicPartition)).get(topicPartition).offset() == 30
        }

        cleanup:
        consumer.close()
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorHandlingSpec')
    @KafkaListener(offsetReset = EARLIEST, groupId = "myGroup", offsetStrategy = SYNC)
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

    @Requires(property = 'spec.name', value = 'KafkaErrorHandlingSpec')
    @KafkaClient
    static interface ErrorClient {
        @Topic("errors")
        void sendMessage(String message)
    }

    KafkaConsumer<byte[], String> createKafkaConsumer() {
        Properties props = new Properties()
        props.setProperty("bootstrap.servers", kafkaContainer.bootstrapServers)
        props.setProperty("enable.auto.commit", "false")
        props.setProperty("auto.offset.reset", "earliest")
        props.setProperty("group.id", "myGroup")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        return new KafkaConsumer<>(props)
    }
}
