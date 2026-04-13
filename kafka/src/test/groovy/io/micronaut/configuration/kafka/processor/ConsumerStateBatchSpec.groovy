package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RecordDeserializationException
import spock.lang.Specification
import sun.misc.Unsafe

import java.lang.reflect.Field
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

    private static ConsumerInfo consumerInfo(OffsetStrategy offsetStrategy) {
        ConsumerInfo info = allocateInstance(ConsumerInfo)
        setField(info, "offsetStrategy", offsetStrategy)
        setField(info, "errorStrategy", ErrorStrategyValue.NONE)
        setField(info, "retryCount", 0)
        setField(info, "shouldHandleAllExceptions", false)
        setField(info, "pollTimeout", Duration.ofMillis(1))
        setField(info, "logMethod", "TestConsumer#receive")
        setField(info, "autoStartup", true)
        return info
    }

    private static <T> T allocateInstance(Class<T> type) {
        return type.cast(Unsafe.class.getMethod("allocateInstance", Class).invoke(unsafe(), type))
    }

    private static void setField(Object target, String fieldName, Object value) {
        Field field = ConsumerInfo.class.getDeclaredField(fieldName)
        field.accessible = true
        field.set(target, value)
    }

    private static Unsafe unsafe() {
        Field field = Unsafe.class.getDeclaredField("theUnsafe")
        field.accessible = true
        return (Unsafe) field.get(null)
    }
}
