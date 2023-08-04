package io.micronaut.configuration.kafka.exceptions

import io.micronaut.context.annotation.Property
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerConfig
import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.apache.kafka.common.TopicPartition

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.DISABLED

class DefaultKafkaListenerExceptionHandlerSpec extends AbstractEmbeddedServerSpec {

    private static final String TOPIC_SEEK = "on-deserialization-error-seek"
    private static final String TOPIC_COMMIT = "on-deserialization-error-commit"
    private static final String TOPIC_NOTHING = "on-deserialization-error-do-nothing"

    void "test seek past record on deserialization error by default"() {
        given:
        StringProducer stringProducer = context.getBean(StringProducer)
        DefaultBehaviorOnDeserializationErrorConsumer consumer = context.getBean(DefaultBehaviorOnDeserializationErrorConsumer)

        when: "A producer sends a message with wrong serialization"
        stringProducer.sendToSeekTopic("not-a-uuid")

        then: "The message is skipped with a seek() but not committed"
        conditions.eventually {
            consumer.currentPosition == 1
            consumer.committedOffset == 0
        }
    }

    void "test commit record on deserialization error"() {
        given:
        StringProducer stringProducer = context.getBean(StringProducer)
        CommitOnDeserializationErrorConsumer consumer = context.getBean(CommitOnDeserializationErrorConsumer)

        when: "A producer sends a message with wrong serialization"
        stringProducer.sendToCommitTopic("not-a-uuid")

        then: "The message is skipped and committed"
        conditions.eventually {
            consumer.currentPosition == 1
            consumer.committedOffset == 1
        }
    }

    void "test do nothing on deserialization error"() {
        given:
        StringProducer stringProducer = context.getBean(StringProducer)
        DoNothingOnDeserializationErrorConsumer consumer = context.getBean(DoNothingOnDeserializationErrorConsumer)

        when: "A producer sends a message with wrong serialization"
        stringProducer.sendToDoNothingTopic("not-a-uuid")

        then: "The message is skipped, but not committed"
        conditions.eventually {
            consumer.currentPosition > 0
            consumer.committedOffset == 0
        }
    }

    static abstract class AbstractOnDeserializationErrorConsumer implements KafkaListenerExceptionHandler {
        Long currentPosition = -1
        Long committedOffset = -1
        DefaultKafkaListenerExceptionHandler errorHandler = new DefaultKafkaListenerExceptionHandler()
        String topic

        AbstractOnDeserializationErrorConsumer(String topic) {
            this.topic = topic
        }

        @Override
        void handle(KafkaListenerException exception) {
            errorHandler.handle(exception)
            TopicPartition tp = new TopicPartition(topic, 0)
            currentPosition = exception.kafkaConsumer.position(tp)
            OffsetAndMetadata committedOffsetAndMetadata = exception.kafkaConsumer.committed(tp)
            if (committedOffsetAndMetadata != null) {
                committedOffset = committedOffsetAndMetadata.offset()
            } else {
                committedOffset = 0
            }
        }
    }

    @Requires(property = 'spec.name', value = 'DefaultKafkaListenerExceptionHandlerSpec')
    @KafkaListener(
            offsetReset = EARLIEST,
            offsetStrategy = DISABLED,
            properties = [
                    @Property(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                            value = "org.apache.kafka.common.serialization.UUIDSerializer")
            ]
    )
    static class DefaultBehaviorOnDeserializationErrorConsumer extends AbstractOnDeserializationErrorConsumer {
        DefaultBehaviorOnDeserializationErrorConsumer() {
            super(TOPIC_SEEK)
        }

        @Topic(TOPIC_SEEK)
        void receive(UUID uuid) {
        }
    }

    @Requires(property = "spec.name", value = "DefaultKafkaListenerExceptionHandlerSpec")
    @KafkaListener(
            offsetReset = EARLIEST,
            offsetStrategy = DISABLED,
            properties = [
                    @Property(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                            value = "org.apache.kafka.common.serialization.UUIDSerializer")
            ]
    )
    static class CommitOnDeserializationErrorConsumer extends AbstractOnDeserializationErrorConsumer {
        CommitOnDeserializationErrorConsumer() {
            super(TOPIC_COMMIT)
            errorHandler.setSkipRecordOnDeserializationFailure(true)
            errorHandler.setCommitRecordOnDeserializationFailure(true)
        }

        @Topic(TOPIC_COMMIT)
        void receive(UUID uuid) {
        }
    }

    @Requires(property = "spec.name", value = "DefaultKafkaListenerExceptionHandlerSpec")
    @KafkaListener(
            offsetReset = EARLIEST,
            offsetStrategy = DISABLED,
            properties = [
                    @Property(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                            value = "org.apache.kafka.common.serialization.UUIDSerializer")
            ]
    )
    static class DoNothingOnDeserializationErrorConsumer extends AbstractOnDeserializationErrorConsumer {
        DoNothingOnDeserializationErrorConsumer() {
            super(TOPIC_NOTHING)
            errorHandler.setSkipRecordOnDeserializationFailure(false)
        }

        @Topic(TOPIC_NOTHING)
        void receive(UUID uuid) {
        }
    }

    @Requires(property = 'spec.name', value = 'DefaultKafkaListenerExceptionHandlerSpec')
    @KafkaClient
    static interface StringProducer {
        @Topic(TOPIC_SEEK)
        void sendToSeekTopic(String message)

        @Topic(TOPIC_COMMIT)
        void sendToCommitTopic(String message)

        @Topic(TOPIC_NOTHING)
        void sendToDoNothingTopic(String message)
    }
}
