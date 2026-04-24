package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.Argument
import io.micronaut.core.type.ReturnType
import io.micronaut.inject.ExecutableMethod
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RecordDeserializationException
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.Collections

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.LOG_AND_RESUME_AT_NEXT_RECORD
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_ON_ERROR

class ConsumerStateBatchSpec extends Specification {

    void "getCurrentRetryCount ignores partitions missing from current offsets"() {
        given:
        ConsumerStateBatch consumerStateBatch = newConsumerStateBatch()
        TopicPartition first = new TopicPartition('topic', 0)
        TopicPartition second = new TopicPartition('topic', 1)

        when:
        int currentRetryCount = invokePrivateMethod(
                consumerStateBatch,
                'getCurrentRetryCount',
                [Set, Map] as Class[],
                [([first, second] as Set), [(first): new OffsetAndMetadata(42L, null)]] as Object[]
        ) as int

        then:
        currentRetryCount == 1
    }

    void "resolveWithErrorStrategy reconstructs missing offsets for the current batch"() {
        given:
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            scheduleTask(_, _) >> { Duration retryDelay, Runnable task -> }
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateBatch consumerState = newConsumerStateBatch(kafkaConsumerProcessor, kafkaConsumer)
        TopicPartition existingPartition = new TopicPartition('topic', 0)
        TopicPartition batchPartition = new TopicPartition('topic', 1)
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = [(existingPartition): new OffsetAndMetadata(5L, null)]
        ConsumerRecords<?, ?> consumerRecords = new ConsumerRecords<>([
                (batchPartition): [new ConsumerRecord<>('topic', 1, 10L, 'key', 'value')]
        ])

        when:
        boolean shouldRetry = invokePrivateMethod(
                consumerState,
                'resolveWithErrorStrategy',
                [ConsumerRecords, Map, ConsumerRecord, Throwable] as Class[],
                [consumerRecords, currentOffsets, null, new RuntimeException('boom')] as Object[]
        ) as boolean

        then:
        shouldRetry
        1 * kafkaConsumer.seek(batchPartition, 10L)
        0 * kafkaConsumer.seek(existingPartition, _)
        0 * kafkaConsumerProcessor.handleException(_, _)
    }

    void "resolveWithErrorStrategy publishes the failed batch to the DLQ and resumes"() {
        given:
        Producer<?, ?> kafkaProducer = Mock(Producer)
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            getProducer('group', String, String) >> kafkaProducer
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateBatch consumerState = newConsumerStateBatch(
            kafkaConsumerProcessor,
            kafkaConsumer,
            kafkaListenerAnnotation(LOG_AND_RESUME_AT_NEXT_RECORD, 'errors-dlq')
        )
        ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>('source-topic', 1, 3L, 'key', 'value')
        ConsumerRecords<?, ?> consumerRecords = new ConsumerRecords<>([
            (new TopicPartition(consumerRecord.topic(), consumerRecord.partition())): [consumerRecord]
        ])

        when:
        boolean shouldRetry = invokePrivateMethod(
            consumerState,
            'resolveWithErrorStrategy',
            [ConsumerRecords, Map, ConsumerRecord, Throwable] as Class[],
            [consumerRecords, [:], null, new RuntimeException('boom')] as Object[]
        ) as boolean

        then:
        !shouldRetry
        1 * kafkaProducer.send({
            ProducerRecord<?, ?> record ->
                record.topic() == 'errors-dlq' &&
                    headerValue(record, 'micronaut-kafka-original-topic') == 'source-topic' &&
                    headerValue(record, 'micronaut-kafka-original-partition') == '1' &&
                    headerValue(record, 'micronaut-kafka-original-offset') == '3'
        }) >> CompletableFuture.completedFuture(null)
        1 * kafkaConsumerProcessor.handleException(_, _)
    }

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

    private ConsumerStateBatch newConsumerStateBatch() {
        newConsumerStateBatch(Mock(KafkaConsumerProcessor), Mock(Consumer) {
            subscription() >> Collections.emptySet()
        })
    }

    private ConsumerStateBatch newConsumerStateBatch(KafkaConsumerProcessor kafkaConsumerProcessor, Consumer<?, ?> kafkaConsumer) {
        newConsumerStateBatch(kafkaConsumerProcessor, kafkaConsumer, kafkaListenerAnnotation())
    }

    private ConsumerStateBatch newConsumerStateBatch(
        KafkaConsumerProcessor kafkaConsumerProcessor,
        Consumer<?, ?> kafkaConsumer,
        AnnotationValue<KafkaListener> kafkaListener
    ) {
        ConsumerInfo consumerInfo = new ConsumerInfo(
                'client',
                'group',
                OffsetStrategy.DISABLED,
                kafkaListener,
                executableMethod()
        )
        new ConsumerStateBatch(kafkaConsumerProcessor, consumerInfo, kafkaConsumer, new Object())
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

    private static Object invokePrivateMethod(Object target, String name, Class[] parameterTypes, Object[] arguments) {
        def method = target.class.getDeclaredMethod(name, parameterTypes)
        method.accessible = true
        method.invoke(target, arguments)
    }

    private AnnotationValue<KafkaListener> kafkaListenerAnnotation() {
        kafkaListenerAnnotation(RETRY_ON_ERROR, null)
    }

    private AnnotationValue<KafkaListener> kafkaListenerAnnotation(def errorStrategy, String dlq) {
        def errorStrategyAnnotation = AnnotationValue.builder(ErrorStrategy)
            .member('value', errorStrategy)
        if (dlq != null) {
            errorStrategyAnnotation.member('dlq', dlq)
        }
        if (errorStrategy == RETRY_ON_ERROR) {
            errorStrategyAnnotation.member('retryCount', 3)
        }
        AnnotationValue.builder(KafkaListener)
                .member('batch', true)
                .member('errorStrategy', errorStrategyAnnotation.build())
                .build()
    }

    private ExecutableMethod<?, ?> executableMethod() {
        ReturnType<?> returnType = Stub() {
            getType() >> void
            isAsyncOrReactive() >> false
            getFirstTypeVariable() >> Optional.empty()
        }
        Stub(ExecutableMethod) {
            getDeclaringType() >> TestBatchListener
            getName() >> 'receive'
            isTrue(KafkaListener, 'batch') >> true
            hasAnnotation(_ as Class) >> false
            getValue(KafkaListener, 'pollTimeout', Duration) >> Optional.of(Duration.ofMillis(100))
            getArguments() >> Argument.ZERO_ARGUMENTS
            stringValues(_ as Class) >> ([] as String[])
            getReturnType() >> returnType
        }
    }

    private static final class TestBatchListener {
    }

    private static String headerValue(ProducerRecord<?, ?> record, String name) {
        new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8)
    }
}
