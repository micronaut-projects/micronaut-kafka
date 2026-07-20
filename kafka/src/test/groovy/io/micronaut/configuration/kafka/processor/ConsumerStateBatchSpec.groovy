package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.bind.ConsumerRecordBinderRegistry
import io.micronaut.configuration.kafka.bind.batch.BatchConsumerRecordsBinderRegistry
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.convert.ConversionService
import io.micronaut.core.type.Argument
import io.micronaut.core.type.ReturnType
import io.micronaut.inject.ExecutableMethod
import io.micronaut.messaging.annotation.SendTo
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
import java.util.Properties

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

    void "resolveWithErrorStrategy stops on exhausted retry for the failed batch"() {
        given:
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor)
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateBatch consumerState = newConsumerStateBatch(
            kafkaConsumerProcessor,
            kafkaConsumer,
            kafkaListenerAnnotation(RETRY_ON_ERROR, null, 0, true)
        )
        TopicPartition firstPartition = new TopicPartition('source-topic', 0)
        TopicPartition secondPartition = new TopicPartition('source-topic', 1)
        ConsumerRecords<?, ?> consumerRecords = new ConsumerRecords<>([
            (firstPartition): [new ConsumerRecord<>('source-topic', 0, 7L, 'key-0', 'value-0')],
            (secondPartition): [new ConsumerRecord<>('source-topic', 1, 9L, 'key-1', 'value-1')]
        ])
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = [(firstPartition): new OffsetAndMetadata(8L, null)]

        when:
        boolean shouldRetry = invokePrivateMethod(
            consumerState,
            'resolveWithErrorStrategy',
            [ConsumerRecords, Map, ConsumerRecord, Throwable] as Class[],
            [consumerRecords, currentOffsets, null, new RuntimeException('boom')] as Object[]
        ) as boolean

        then:
        shouldRetry
        1 * kafkaConsumer.seek(firstPartition, 8L)
        1 * kafkaConsumer.seek(secondPartition, 9L)
        1 * kafkaConsumerProcessor.handleException(_, _)
        pauseRequests(consumerState) == [firstPartition, secondPartition] as Set
    }

    void "resolveWithErrorStrategy stops on exhausted retry for a synthetic failed record"() {
        given:
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor)
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateBatch consumerState = newConsumerStateBatch(
            kafkaConsumerProcessor,
            kafkaConsumer,
            kafkaListenerAnnotation(RETRY_ON_ERROR, null, 0, true)
        )
        ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>('source-topic', 2, 11L, 'key', 'value')
        TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition())
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = [(topicPartition): new OffsetAndMetadata(11L, null)]

        when:
        boolean shouldRetry = invokePrivateMethod(
            consumerState,
            'resolveWithErrorStrategy',
            [ConsumerRecords, Map, ConsumerRecord, Throwable] as Class[],
            [null, currentOffsets, consumerRecord, new RuntimeException('boom')] as Object[]
        ) as boolean

        then:
        shouldRetry
        1 * kafkaConsumer.seek(topicPartition, 11L)
        1 * kafkaConsumerProcessor.handleException(_, _)
        pauseRequests(consumerState) == [topicPartition] as Set
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
                    it.kafkaConsumer.is(kafkaConsumer) &&
                    it.consumerRecord.present &&
                    it.consumerRecord.get().topic() == 'books' &&
                    it.consumerRecord.get().partition() == 1 &&
                    it.consumerRecord.get().offset() == 4L
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
        1 * kafkaConsumerProcessor.handleException(_, {
            it instanceof KafkaListenerException &&
                    it.cause.is(exception) &&
                    it.kafkaConsumer.is(kafkaConsumer) &&
                    it.consumerRecord.present &&
                    it.consumerRecord.get().topic() == 'books' &&
                    it.consumerRecord.get().partition() == 1 &&
                    it.consumerRecord.get().offset() == 4L
        })
    }

    void "filtered batches skip listener invocation"() {
        given:
        TopicPartition topicPartition = new TopicPartition('source-topic', 1)
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>('source-topic', 1, 3L, 'key', 'value')
        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>([(topicPartition): [consumerRecord]])
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            interceptRecords(_, consumerRecords) >> ConsumerRecords.empty()
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateBatch consumerState = newConsumerStateBatch(
            kafkaConsumerProcessor,
            kafkaConsumer,
            kafkaListenerAnnotation(LOG_AND_RESUME_AT_NEXT_RECORD, 'errors-dlq'),
            executableMethod([Argument.of(ConsumerRecords)] as Argument[]) { throw new AssertionError('listener should not be invoked') }
        )

        when:
        consumerState.processRecords(consumerRecords, [:])

        then:
        0 * kafkaConsumerProcessor.handleException(_, _)
    }

    void "send-to-transaction uses original batch offsets when trailing records are filtered"() {
        given:
        TopicPartition topicPartition = new TopicPartition('source-topic', 1)
        ConsumerRecord<String, String> kept = new ConsumerRecord<>('source-topic', 1, 3L, 'key-1', 'keep')
        ConsumerRecord<String, String> skipped = new ConsumerRecord<>('source-topic', 1, 4L, 'key-2', 'skip')
        ConsumerRecords<String, String> originalRecords = new ConsumerRecords<>([(topicPartition): [kept, skipped]])
        ConsumerRecords<String, String> interceptedRecords = new ConsumerRecords<>([(topicPartition): [kept]])
        Producer<?, ?> kafkaProducer = Mock(Producer)
        ConsumerRecordBinderRegistry binderRegistry = new ConsumerRecordBinderRegistry(ConversionService.SHARED)
        BatchConsumerRecordsBinderRegistry batchBinderRegistry = new BatchConsumerRecordsBinderRegistry(binderRegistry, ConversionService.SHARED)
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            getBatchBinderRegistry() >> batchBinderRegistry
            interceptRecords(_, originalRecords) >> interceptedRecords
            getTransactionalProducer(_, 'tx-id', byte[].class, Object.class) >> kafkaProducer
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateBatch consumerState = newConsumerStateBatch(
            kafkaConsumerProcessor,
            kafkaConsumer,
            OffsetStrategy.SEND_TO_TRANSACTION,
            transactionalKafkaListenerAnnotation(),
            sendToExecutableMethod { ['out'] }
        )

        when:
        consumerState.processRecords(originalRecords, [:])

        then:
        1 * kafkaProducer.beginTransaction()
        1 * kafkaProducer.send({
            ProducerRecord<?, ?> record ->
                record.topic() == 'target' &&
                    record.key() == 'key-1' &&
                    record.value() == 'out'
        }, _) >> CompletableFuture.completedFuture(null)
        1 * kafkaProducer.sendOffsetsToTransaction({
            Map<TopicPartition, OffsetAndMetadata> offsets ->
                offsets[topicPartition]?.offset() == 5L
        }, _)
        1 * kafkaProducer.commitTransaction()
        0 * kafkaConsumerProcessor.handleException(_, _)
    }

    void "fully filtered send-to-transaction batches still commit original offsets"() {
        given:
        TopicPartition topicPartition = new TopicPartition('source-topic', 1)
        ConsumerRecord<String, String> first = new ConsumerRecord<>('source-topic', 1, 3L, 'key-1', 'one')
        ConsumerRecord<String, String> second = new ConsumerRecord<>('source-topic', 1, 4L, 'key-2', 'two')
        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>([(topicPartition): [first, second]])
        Producer<?, ?> kafkaProducer = Mock(Producer)
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            interceptRecords(_, consumerRecords) >> ConsumerRecords.empty()
            getTransactionalProducer(_, 'tx-id', byte[].class, Object.class) >> kafkaProducer
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateBatch consumerState = newConsumerStateBatch(
            kafkaConsumerProcessor,
            kafkaConsumer,
            OffsetStrategy.SEND_TO_TRANSACTION,
            transactionalKafkaListenerAnnotation(),
            sendToExecutableMethod { throw new AssertionError('listener should not be invoked') }
        )

        when:
        consumerState.processRecords(consumerRecords, [:])

        then:
        1 * kafkaProducer.beginTransaction()
        0 * kafkaProducer.send(_, _)
        1 * kafkaProducer.sendOffsetsToTransaction({
            Map<TopicPartition, OffsetAndMetadata> offsets ->
                offsets[topicPartition]?.offset() == 5L
        }, _)
        1 * kafkaProducer.commitTransaction()
        0 * kafkaConsumerProcessor.handleException(_, _)
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
        newConsumerStateBatch(kafkaConsumerProcessor, kafkaConsumer, kafkaListener, executableMethod())
    }

    private ConsumerStateBatch newConsumerStateBatch(
        KafkaConsumerProcessor kafkaConsumerProcessor,
        Consumer<?, ?> kafkaConsumer,
        AnnotationValue<KafkaListener> kafkaListener,
        ExecutableMethod<?, ?> executableMethod
    ) {
        newConsumerStateBatch(kafkaConsumerProcessor, kafkaConsumer, OffsetStrategy.DISABLED, kafkaListener, executableMethod)
    }

    private ConsumerStateBatch newConsumerStateBatch(
        KafkaConsumerProcessor kafkaConsumerProcessor,
        Consumer<?, ?> kafkaConsumer,
        OffsetStrategy offsetStrategy,
        AnnotationValue<KafkaListener> kafkaListener,
        ExecutableMethod<?, ?> executableMethod
    ) {
        ConsumerInfo consumerInfo = new ConsumerInfo(
                'client',
                'group',
                offsetStrategy,
                kafkaListener,
                new Properties(),
                executableMethod,
                []
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
        return new ConsumerInfo("test-client", null, offsetStrategy, kafkaListener, new Properties(), method, [])
    }

    private static Object invokePrivateMethod(Object target, String name, Class[] parameterTypes, Object[] arguments) {
        def method = target.class.getDeclaredMethod(name, parameterTypes)
        method.accessible = true
        method.invoke(target, arguments)
    }

    private AnnotationValue<KafkaListener> kafkaListenerAnnotation() {
        kafkaListenerAnnotation(RETRY_ON_ERROR, null)
    }

    private AnnotationValue<KafkaListener> kafkaListenerAnnotation(
        def errorStrategy,
        String dlq,
        Integer retryCount = null,
        boolean stopOnExhaustedRetry = false
    ) {
        def errorStrategyAnnotation = AnnotationValue.builder(ErrorStrategy)
            .member('value', errorStrategy)
        if (dlq != null) {
            errorStrategyAnnotation.member('dlq', dlq)
        }
        if (errorStrategy == RETRY_ON_ERROR) {
            errorStrategyAnnotation.member('retryCount', retryCount == null ? 3 : retryCount)
        }
        if (stopOnExhaustedRetry) {
            errorStrategyAnnotation.member('stopOnExhaustedRetry', true)
        }
        AnnotationValue.builder(KafkaListener)
                .member('batch', true)
                .member('errorStrategy', errorStrategyAnnotation.build())
                .build()
    }

    private AnnotationValue<KafkaListener> transactionalKafkaListenerAnnotation() {
        AnnotationValue.builder(KafkaListener)
            .member('batch', true)
            .member('producerTransactionalId', 'tx-id')
            .build()
    }

    private ExecutableMethod<?, ?> executableMethod() {
        executableMethod(Argument.ZERO_ARGUMENTS) { null }
    }

    private ExecutableMethod<?, ?> executableMethod(Argument[] arguments, Closure<?> invocation) {
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
            getArguments() >> arguments
            stringValues(_ as Class) >> ([] as String[])
            getReturnType() >> returnType
            invoke(_, _ as Object[]) >> { Object instance, Object[] args ->
                invocation.call(([instance] + args) as Object[])
            }
            invoke(_) >> { Object instance ->
                invocation.call(([instance]) as Object[])
            }
        }
    }

    private ExecutableMethod<?, ?> sendToExecutableMethod(Closure<?> invocation) {
        ReturnType<?> returnType = Stub() {
            getType() >> List
            isAsyncOrReactive() >> false
            getFirstTypeVariable() >> Optional.empty()
        }
        Stub(ExecutableMethod) {
            getDeclaringType() >> TestBatchListener
            getName() >> 'receive'
            isTrue(KafkaListener, 'batch') >> true
            hasAnnotation(_ as Class) >> { Class<?> annotation -> annotation == SendTo }
            getValue(KafkaListener, 'pollTimeout', Duration) >> Optional.of(Duration.ofMillis(100))
            getArguments() >> Argument.ZERO_ARGUMENTS
            stringValues(_ as Class) >> { Class<?> annotation -> annotation == SendTo ? (['target'] as String[]) : ([] as String[]) }
            getReturnType() >> returnType
            invoke(_, _ as Object[]) >> { Object instance, Object[] args ->
                invocation.call(([instance] + args) as Object[])
            }
            invoke(_) >> { Object instance ->
                invocation.call(([instance]) as Object[])
            }
        }
    }

    private static Set<TopicPartition> pauseRequests(ConsumerStateBatch consumerState) {
        def field = ConsumerState.getDeclaredField('pauseRequests')
        field.accessible = true
        field.get(consumerState) as Set<TopicPartition>
    }

    private static final class TestBatchListener {
    }

    private static String headerValue(ProducerRecord<?, ?> record, String name) {
        new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8)
    }
}
