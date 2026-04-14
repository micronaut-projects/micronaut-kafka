package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.Argument
import io.micronaut.core.type.ReturnType
import io.micronaut.inject.ExecutableMethod
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RecordDeserializationException
import spock.lang.Specification

import java.time.Duration
import java.util.Collections

class ConsumerStateBatchSpec extends Specification {

    void "does not seek past deserialization failures when offset strategy is disabled"() {
        given:
        TopicPartition topicPartition = new TopicPartition("books", 1)
        RecordDeserializationException exception = new RecordDeserializationException(
                topicPartition,
                4L,
                "boom",
                new IllegalStateException("deserialization failed")
        )
        Consumer<?, ?> kafkaConsumer = Mock() {
            subscription() >> Collections.emptySet()
            poll(_ as Duration) >> { throw exception }
        }
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock()
        ConsumerInfo info = consumerInfo(OffsetStrategy.DISABLED)
        ConsumerStateBatch state = new ConsumerStateBatch(kafkaConsumerProcessor, info, kafkaConsumer, new Object())

        when:
        ConsumerRecords<?, ?> records = state.pollRecords(null)

        then:
        records == null
        0 * kafkaConsumer.seek(_, _)
        1 * kafkaConsumerProcessor.handleException(_, {
            it instanceof KafkaListenerException &&
                    it.cause.is(exception) &&
                    it.kafkaConsumer.is(kafkaConsumer)
        })
    }

    void "continues to seek past deserialization failures for non-disabled offset strategies"() {
        given:
        TopicPartition topicPartition = new TopicPartition("books", 1)
        RecordDeserializationException exception = new RecordDeserializationException(
                topicPartition,
                4L,
                "boom",
                new IllegalStateException("deserialization failed")
        )
        Consumer<?, ?> kafkaConsumer = Mock() {
            subscription() >> Collections.emptySet()
            poll(_ as Duration) >> { throw exception }
        }
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock()
        ConsumerInfo info = consumerInfo(OffsetStrategy.SYNC)
        ConsumerStateBatch state = new ConsumerStateBatch(kafkaConsumerProcessor, info, kafkaConsumer, new Object())

        when:
        ConsumerRecords<?, ?> records = state.pollRecords(null)

        then:
        records == null
        1 * kafkaConsumer.seek(topicPartition, 5L)
        1 * kafkaConsumerProcessor.handleException(_, _ as KafkaListenerException)
    }

    private ConsumerInfo consumerInfo(OffsetStrategy offsetStrategy) {
        AnnotationValue<KafkaListener> kafkaListener = AnnotationValue.builder(KafkaListener).build()
        ReturnType<?> returnType = Stub(ReturnType) {
            getType() >> Void
            isAsyncOrReactive() >> false
            getFirstTypeVariable() >> Optional.empty()
        }
        ExecutableMethod<?, ?> method = Stub(ExecutableMethod) {
            getDeclaringType() >> ConsumerStateBatchSpec
            getName() >> "receive"
            isTrue(_, _) >> false
            hasAnnotation(_) >> false
            getValue(KafkaListener, "pollTimeout", Duration) >> Optional.of(Duration.ofMillis(1))
            getArguments() >> new Argument[0]
            stringValues(_) >> null
            getReturnType() >> returnType
        }
        return new ConsumerInfo("test-client", null, offsetStrategy, kafkaListener, method)
    }
}
