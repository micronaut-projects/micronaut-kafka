package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC_PER_RECORD
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class KafkaShutdownHandlingSpec extends AbstractEmbeddedServerSpec {

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                [(EMBEDDED_TOPICS): ["wakeup", "wakeup-batch", "wakeup-with-successful-messages"]]
    }

    void "test wakeup does not commit"() {
        given:
            WakeupClient myClient = context.getBean(WakeupClient)
            WakeupConsumer wakeupConsumer = context.getBean(WakeupConsumer)
            KafkaConsumer<byte[], String> consumer = createKafkaConsumer()

        when:"An exception occurs when consumer shuts down"
            myClient.sendMessage("One")
            myClient.sendMessage("Two")

            ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1)
            schedule.schedule(() -> {
                wakeupConsumer.wakeup()
            }, 100, TimeUnit.MILLISECONDS)

            // wait a moment for first wakeup / consumer to close
            sleep(1_000)

        then:"The messages are not committed"
            TopicPartition topicPartition = new TopicPartition("wakeup", 0)
            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = consumer.committed([topicPartition] as Set)
            offsetAndMetadata == null
                    || offsetAndMetadata[topicPartition] == null
                    || offsetAndMetadata[topicPartition].offset() == 0

        cleanup:
            consumer.close()
    }

    void "test wakeup does not commit messages that have not been processed"() {
        given:
            WakeupSuccessfulMessagesClient myClient = context.getBean(WakeupSuccessfulMessagesClient)
            WakeupSuccessfulMessagesConsumer wakeupConsumer = context.getBean(WakeupSuccessfulMessagesConsumer)
            KafkaConsumer<byte[], String> consumer = createKafkaConsumer()

            when: "A consumer shuts down before processing all the messages"
            for (i in 0..<100) {
                myClient.sendLongTimeProcessingMessage("Message#${i}")
            }

            ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1)
            schedule.schedule(() -> {
                wakeupConsumer.wakeup()
            }, 100, TimeUnit.MILLISECONDS)

            // wait a moment for first wakeup / consumer to close
            sleep(5_000)

        then: "The messages are not committed"
            TopicPartition topicPartition = new TopicPartition("wakeup-with-successful-messages", 0)
            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = consumer.committed([topicPartition] as Set)
            offsetAndMetadata != null
                    && offsetAndMetadata[topicPartition] != null
                    && offsetAndMetadata[topicPartition].offset() <= wakeupConsumer.received.unique().size()

        cleanup:
            consumer.close()
    }

    void "test batch shut down does not commit"() {
        given:
            WakeupBatchClient myClient = context.getBean(WakeupBatchClient)
            WakeupBatchConsumer wakeupConsumer = context.getBean(WakeupBatchConsumer)
            KafkaConsumer<byte[], String> consumer = createKafkaConsumer()

        when:"An exception occurs when consumer shuts down"
            myClient.sendMessage("One")
            myClient.sendMessage("Two")

            ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1)
            schedule.schedule(() -> {
                wakeupConsumer.wakeup()
            }, 100, TimeUnit.MILLISECONDS)

            // wait a moment for first wakeup / consumer to close
            sleep(1_000)


        then:"The messages are not committed"
            TopicPartition topicPartition = new TopicPartition("wakeup-batch", 0)
            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = consumer.committed([topicPartition] as Set)
            offsetAndMetadata == null
                    || offsetAndMetadata[topicPartition] == null
                    || offsetAndMetadata[topicPartition].offset() == 0

        cleanup:
            consumer.close()
    }

    @Requires(property = 'spec.name', value = 'KafkaShutdownHandlingSpec')
    @KafkaListener(clientId = "shutdown-spec-wakeup-consumer", groupId = "myGroup", offsetReset = EARLIEST, offsetStrategy = SYNC)
    static class WakeupConsumer implements ConsumerAware {
        Consumer kafkaConsumer
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []

        @Topic("wakeup")
        void handleMessage(String message) {
            sleep(500)
            if (count.getAndIncrement() == 0) {
                throw new RuntimeException("Won't handle first")
            }

            received << message
        }

        @Override
        void setKafkaConsumer(Consumer consumer) {
            kafkaConsumer = consumer
        }

        void wakeup() {
            kafkaConsumer.wakeup()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaShutdownHandlingSpec')
    @KafkaListener(
            clientId = "wakeup-successful-messages-consumer",
            groupId = "myGroup", offsetReset = EARLIEST,
            offsetStrategy = SYNC_PER_RECORD
    )
    static class WakeupSuccessfulMessagesConsumer implements ConsumerAware {

        public static final int SLEEP_TIME = 100 // ms

        Consumer kafkaConsumer
        List<String> received = []
        private static final Logger LOGGER = LoggerFactory.getLogger(WakeupSuccessfulMessagesConsumer.class);

        @Topic("wakeup-with-successful-messages")
        void handle(String message) {
            LOGGER.debug("Started processing message: $message")
            sleep(SLEEP_TIME)
            received << message
            LOGGER.debug("Finished processing message: $message")
        }

        @Override
        void setKafkaConsumer(Consumer consumer) {
            kafkaConsumer = consumer
        }

        void wakeup() {
            kafkaConsumer.wakeup()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaShutdownHandlingSpec')
    @KafkaListener(
            clientId = "shutdown-spec-wakeup-batch-consumer",
            groupId = "myGroup",
            offsetReset = EARLIEST,
            offsetStrategy = SYNC,
            batch = true
    )
    static class WakeupBatchConsumer implements ConsumerAware {
        Consumer kafkaConsumer
        AtomicInteger count = new AtomicInteger(0)
        List<String> received = []

        @Topic("wakeup-batch")
        void handleMessage(List<String> messages) {
            for (String message : messages) {
                sleep(500)
                if (count.getAndIncrement() == 0) {
                    throw new RuntimeException("Won't handle first")
                }

                received << message
            }
        }

        @Override
        void setKafkaConsumer(Consumer consumer) {
            kafkaConsumer = consumer
        }

        void wakeup() {
            kafkaConsumer.wakeup()
        }
    }

    KafkaConsumer<byte[], String> createKafkaConsumer() {
        Properties props = new Properties()
        props.setProperty("bootstrap.servers", bootstrapServers)
        props.setProperty("enable.auto.commit", "false")
        props.setProperty("auto.offset.reset", "earliest")
        props.setProperty("group.id", "myGroup")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        return new KafkaConsumer<>(props)
    }

    @Requires(property = 'spec.name', value = 'KafkaShutdownHandlingSpec')
    @KafkaClient
    static interface WakeupClient {
        @Topic("wakeup")
        void sendMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaShutdownHandlingSpec')
    @KafkaClient
    static interface WakeupSuccessfulMessagesClient {
        @Topic("wakeup-with-successful-messages")
        void sendLongTimeProcessingMessage(String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaShutdownHandlingSpec')
    @KafkaClient
    static interface WakeupBatchClient {
        @Topic("wakeup-batch")
        void sendMessage(String message)
    }
}
