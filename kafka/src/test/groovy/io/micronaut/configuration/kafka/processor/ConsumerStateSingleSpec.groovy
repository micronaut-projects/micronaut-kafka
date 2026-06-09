package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.bind.ConsumerRecordBinderRegistry
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.Argument
import io.micronaut.core.type.ReturnType
import io.micronaut.inject.ExecutableMethod
import io.micronaut.messaging.exceptions.MessagingSystemException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RecordDeserializationException
import spock.lang.Specification

import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Optional
import java.util.Properties
import java.util.concurrent.CompletableFuture

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.NONE
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.LOG_AND_RESUME_AT_NEXT_RECORD
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_ON_ERROR

class ConsumerStateSingleSpec extends Specification {

    void "reset the following partitions seeks each topic partition independently"() {
        given:
        List<TopicPartition> seeks = []
        List<Long> offsets = []
        Consumer consumer = createConsumer(seeks, offsets)
        ConsumerStateSingle state = new ConsumerStateSingle(null, buildConsumerInfo(), consumer, new Object())
        Method method = ConsumerStateSingle.getDeclaredMethod("resetTheFollowingPartitions", ConsumerRecord, Iterator)
        method.accessible = true

        when:
        method.invoke(state, [
            new ConsumerRecord<>("topic-a", 0, 5L, null, "error"),
            [
                new ConsumerRecord<>("topic-b", 0, 7L, null, "retry-other-topic"),
                new ConsumerRecord<>("topic-b", 0, 8L, null, "same-partition-second-record"),
                new ConsumerRecord<>("topic-a", 1, 9L, null, "other-partition"),
                new ConsumerRecord<>("topic-c", 0, 1L, null, "third-topic")
            ].iterator()
        ] as Object[])

        then:
        seeks == [
            new TopicPartition("topic-b", 0),
            new TopicPartition("topic-a", 1),
            new TopicPartition("topic-c", 0)
        ]
        offsets == [7L, 9L, 1L]
    }

    void "resolveWithErrorStrategy publishes the failed record to the DLQ and resumes"() {
        given:
        Producer<?, ?> kafkaProducer = Mock(Producer)
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            getProducer('group', String, String) >> kafkaProducer
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateSingle consumerState = newConsumerStateSingle(kafkaConsumerProcessor, kafkaConsumer)
        ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>('source-topic', 2, 7L, 'key', 'value')
        consumerRecord.headers().add('micronaut-kafka-exception-class', 'stale-class'.getBytes(StandardCharsets.UTF_8))
        consumerRecord.headers().add('micronaut-kafka-exception-message', 'stale-message'.getBytes(StandardCharsets.UTF_8))
        consumerRecord.headers().add('micronaut-kafka-original-topic', 'stale-topic'.getBytes(StandardCharsets.UTF_8))
        consumerRecord.headers().add('micronaut-kafka-original-partition', '999'.getBytes(StandardCharsets.UTF_8))
        consumerRecord.headers().add('micronaut-kafka-original-offset', '1234'.getBytes(StandardCharsets.UTF_8))
        ConsumerRecords<?, ?> consumerRecords = new ConsumerRecords<>([
            (new TopicPartition(consumerRecord.topic(), consumerRecord.partition())): [consumerRecord]
        ])
        RuntimeException error = new RuntimeException('boom')

        when:
        boolean shouldRetry = invokePrivateMethod(
            consumerState,
            'resolveWithErrorStrategy',
            [ConsumerRecords, ConsumerRecord, Throwable] as Class[],
            [consumerRecords, consumerRecord, error] as Object[]
        ) as boolean

        then:
        !shouldRetry
        1 * kafkaProducer.send({
            ProducerRecord<?, ?> record ->
                record.topic() == 'errors-dlq' &&
                    record.key() == 'key' &&
                    record.value() == 'value' &&
                    headerValue(record, 'micronaut-kafka-exception-class') == RuntimeException.name &&
                    headerCount(record, 'micronaut-kafka-exception-class') == 1 &&
                    headerValue(record, 'micronaut-kafka-exception-message') == 'boom' &&
                    headerCount(record, 'micronaut-kafka-exception-message') == 1 &&
                    headerValue(record, 'micronaut-kafka-original-topic') == 'source-topic' &&
                    headerCount(record, 'micronaut-kafka-original-topic') == 1 &&
                    headerValue(record, 'micronaut-kafka-original-partition') == '2' &&
                    headerCount(record, 'micronaut-kafka-original-partition') == 1 &&
                    headerValue(record, 'micronaut-kafka-original-offset') == '7' &&
                    headerCount(record, 'micronaut-kafka-original-offset') == 1
        }) >> CompletableFuture.completedFuture(null)
        1 * kafkaConsumerProcessor.handleException(_, _)
    }

    void "consumer info requires dlq for log and resume strategy"() {
        when:
        new ConsumerInfo(
            'client',
            'group',
            OffsetStrategy.DISABLED,
            kafkaListenerAnnotation(LOG_AND_RESUME_AT_NEXT_RECORD, null),
            new Properties(),
            executableMethod()
        )

        then:
        def ex = thrown(MessagingSystemException)
        ex.message.contains('LOG_AND_RESUME_AT_NEXT_RECORD')
        ex.message.contains('dlq')
    }

    void "consumer info requires a retry strategy to stop on exhausted retry"() {
        when:
        new ConsumerInfo(
            'client',
            'group',
            OffsetStrategy.DISABLED,
            kafkaListenerAnnotation(RESUME_AT_NEXT_RECORD, null, null, true),
            new Properties(),
            executableMethod()
        )

        then:
        def ex = thrown(MessagingSystemException)
        ex.message.contains('stopOnExhaustedRetry')
        ex.message.contains('retry error strategy')
    }

    void "poll-time deserialization failures expose a synthetic consumer record to the exception handler"() {
        given:
        TopicPartition topicPartition = new TopicPartition('books', 1)
        RecordDeserializationException exception = new RecordDeserializationException(
            topicPartition,
            4L,
            'boom',
            new IllegalStateException('deserialization failed')
        )
        Consumer<?, ?> kafkaConsumer = Mock() {
            subscription() >> Collections.emptySet()
            poll(_ as Duration) >> { throw exception }
        }
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock()
        ConsumerStateSingle state = newConsumerStateSingle(kafkaConsumerProcessor, kafkaConsumer)

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

    void "sync per record commits the failed record offset when the error strategy resumes"() {
        given:
        TopicPartition topicPartition = new TopicPartition('source-topic', 2)
        ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>('source-topic', 2, 7L, 'key', 'value')
        ConsumerRecords<?, ?> consumerRecords = new ConsumerRecords<>([(topicPartition): [consumerRecord]])
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            getBinderRegistry() >> Stub(ConsumerRecordBinderRegistry)
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateSingle consumerState = newConsumerStateSingle(
            kafkaConsumerProcessor,
            kafkaConsumer,
            OffsetStrategy.SYNC_PER_RECORD,
            kafkaListenerAnnotation(RESUME_AT_NEXT_RECORD),
            executableMethod { throw new IllegalStateException('boom') }
        )
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = [:]

        when:
        consumerState.processRecords(consumerRecords, currentOffsets)

        then:
        currentOffsets[topicPartition].offset() == 8L
        1 * kafkaConsumer.commitSync({
            Map<TopicPartition, OffsetAndMetadata> offsets ->
                offsets[topicPartition]?.offset() == 8L
        })
        1 * kafkaConsumerProcessor.handleException(_, _)
    }

    void "sync per record does not commit the failed record offset while a retry is scheduled"() {
        given:
        TopicPartition topicPartition = new TopicPartition('source-topic', 2)
        ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>('source-topic', 2, 7L, 'key', 'value')
        ConsumerRecords<?, ?> consumerRecords = new ConsumerRecords<>([(topicPartition): [consumerRecord]])
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            getBinderRegistry() >> Stub(ConsumerRecordBinderRegistry)
            scheduleTask(_, _) >> { Duration retryDelay, Runnable task -> }
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateSingle consumerState = newConsumerStateSingle(
            kafkaConsumerProcessor,
            kafkaConsumer,
            OffsetStrategy.SYNC_PER_RECORD,
            kafkaListenerAnnotation(RETRY_ON_ERROR, null, 1),
            executableMethod { throw new IllegalStateException('boom') }
        )

        when:
        consumerState.processRecords(consumerRecords, [:])

        then:
        1 * kafkaConsumer.seek(topicPartition, 7L)
        0 * kafkaConsumer.commitSync(_)
        0 * kafkaConsumerProcessor.handleException(_, _)
    }

    void "sync per record commits the failed record offset once retries are exhausted"() {
        given:
        TopicPartition topicPartition = new TopicPartition('source-topic', 2)
        ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>('source-topic', 2, 7L, 'key', 'value')
        ConsumerRecords<?, ?> consumerRecords = new ConsumerRecords<>([(topicPartition): [consumerRecord]])
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            getBinderRegistry() >> Stub(ConsumerRecordBinderRegistry)
            scheduleTask(_, _) >> { Duration retryDelay, Runnable task -> }
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateSingle consumerState = newConsumerStateSingle(
            kafkaConsumerProcessor,
            kafkaConsumer,
            OffsetStrategy.SYNC_PER_RECORD,
            kafkaListenerAnnotation(RETRY_ON_ERROR, null, 1),
            executableMethod { throw new IllegalStateException('boom') }
        )

        when:
        consumerState.processRecords(consumerRecords, [:])
        consumerState.processRecords(consumerRecords, [:])

        then:
        1 * kafkaConsumer.seek(topicPartition, 7L)
        1 * kafkaConsumer.commitSync({
            Map<TopicPartition, OffsetAndMetadata> offsets ->
                offsets[topicPartition]?.offset() == 8L
        })
        1 * kafkaConsumerProcessor.handleException(_, _)
    }

    void "sync per record does not commit the failed record offset for none error strategy"() {
        given:
        TopicPartition topicPartition = new TopicPartition('source-topic', 2)
        ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>('source-topic', 2, 7L, 'key', 'value')
        ConsumerRecords<?, ?> consumerRecords = new ConsumerRecords<>([(topicPartition): [consumerRecord]])
        KafkaConsumerProcessor kafkaConsumerProcessor = Mock(KafkaConsumerProcessor) {
            getBinderRegistry() >> Stub(ConsumerRecordBinderRegistry)
        }
        Consumer<?, ?> kafkaConsumer = Mock(Consumer) {
            subscription() >> Collections.emptySet()
        }
        ConsumerStateSingle consumerState = newConsumerStateSingle(
            kafkaConsumerProcessor,
            kafkaConsumer,
            OffsetStrategy.SYNC_PER_RECORD,
            kafkaListenerAnnotation(NONE),
            executableMethod { throw new IllegalStateException('boom') }
        )

        when:
        consumerState.processRecords(consumerRecords, [:])

        then:
        0 * kafkaConsumer.commitSync(_)
        1 * kafkaConsumerProcessor.handleException(_, _)
    }

    private static Consumer createConsumer(List<TopicPartition> seeks, List<Long> offsets) {
        Proxy.newProxyInstance(
            ConsumerStateSingleSpec.classLoader,
            [Consumer] as Class<?>[],
            { _, method, args ->
                if (method.name == "subscription") {
                    return Collections.emptySet()
                }
                if (method.name == "seek") {
                    seeks << (TopicPartition) args[0]
                    offsets << (Long) args[1]
                    return null
                }
                defaultValue(method.returnType)
            }
        ) as Consumer
    }

    private static ConsumerInfo buildConsumerInfo() {
        ReturnType<?> returnType = Proxy.newProxyInstance(
            ConsumerStateSingleSpec.classLoader,
            [ReturnType] as Class<?>[],
            { _, method, _ ->
                switch (method.name) {
                    case "getType":
                        return Void.TYPE
                    case "isAsyncOrReactive":
                        return false
                    case "getFirstTypeVariable":
                        return Optional.empty()
                    default:
                        return defaultValue(method.returnType)
                }
            }
        ) as ReturnType<?>
        ExecutableMethod<?, ?> executableMethod = Proxy.newProxyInstance(
            ConsumerStateSingleSpec.classLoader,
            [ExecutableMethod] as Class<?>[],
            { _, method, _ ->
                switch (method.name) {
                    case "getDeclaringType":
                        return ConsumerStateSingleSpec
                    case "getName":
                        return "handleMessage"
                    case "isTrue":
                        return false
                    case "hasAnnotation":
                        return false
                    case "getValue":
                        return Optional.empty()
                    case "getArguments":
                        return Argument.ZERO_ARGUMENTS
                    case "stringValues":
                        return [] as String[]
                    case "getDeclaredAnnotationValuesByType":
                        return []
                    case "getReturnType":
                        return returnType
                    default:
                        return defaultValue(method.returnType)
                }
            }
        ) as ExecutableMethod<?, ?>
        AnnotationValue<KafkaListener> annotation = AnnotationValue.builder(KafkaListener).build()
        new ConsumerInfo("test-client", "test-group", OffsetStrategy.SYNC, annotation, new Properties(), executableMethod)
    }

    private ConsumerStateSingle newConsumerStateSingle(KafkaConsumerProcessor kafkaConsumerProcessor, Consumer<?, ?> kafkaConsumer) {
        newConsumerStateSingle(
            kafkaConsumerProcessor,
            kafkaConsumer,
            OffsetStrategy.DISABLED,
            kafkaListenerAnnotation(),
            executableMethod()
        )
    }

    private ConsumerStateSingle newConsumerStateSingle(
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
            executableMethod
        )
        new ConsumerStateSingle(kafkaConsumerProcessor, consumerInfo, kafkaConsumer, new Object())
    }

    private static Object invokePrivateMethod(Object target, String name, Class[] parameterTypes, Object[] arguments) {
        def method = target.class.getDeclaredMethod(name, parameterTypes)
        method.accessible = true
        method.invoke(target, arguments)
    }

    private AnnotationValue<KafkaListener> kafkaListenerAnnotation(
        def errorStrategy = LOG_AND_RESUME_AT_NEXT_RECORD,
        String dlq = 'errors-dlq',
        Integer retryCount = null,
        boolean stopOnExhaustedRetry = false
    ) {
        def errorStrategyAnnotation = AnnotationValue.builder(ErrorStrategy)
            .member('value', errorStrategy)
        if (dlq != null) {
            errorStrategyAnnotation.member('dlq', dlq)
        }
        if (retryCount != null) {
            errorStrategyAnnotation.member('retryCount', retryCount)
        }
        if (stopOnExhaustedRetry) {
            errorStrategyAnnotation.member('stopOnExhaustedRetry', true)
        }
        AnnotationValue.builder(KafkaListener)
            .member('errorStrategy', errorStrategyAnnotation.build())
            .build()
    }

    private ExecutableMethod<?, ?> executableMethod() {
        executableMethod { null }
    }

    private ExecutableMethod<?, ?> executableMethod(Closure<?> invocation) {
        ReturnType<?> returnType = Stub() {
            getType() >> void
            isAsyncOrReactive() >> false
            getFirstTypeVariable() >> Optional.empty()
        }
        Proxy.newProxyInstance(
            ConsumerStateSingleSpec.classLoader,
            [ExecutableMethod] as Class<?>[],
            { _, method, _ ->
                switch (method.name) {
                    case 'getDeclaringType':
                        return TestListener
                    case 'getName':
                        return 'receive'
                    case 'isTrue':
                        return false
                    case 'hasAnnotation':
                        return false
                    case 'getValue':
                        return Optional.of(Duration.ofMillis(100))
                    case 'getArguments':
                        return Argument.ZERO_ARGUMENTS
                    case 'stringValues':
                        return [] as String[]
                    case 'getDeclaredAnnotationValuesByType':
                        return []
                    case 'getReturnType':
                        return returnType
                    case 'invoke':
                        return invocation.call()
                    default:
                        return defaultValue(method.returnType)
                }
            }
        ) as ExecutableMethod<?, ?>
    }

    private static String headerValue(ProducerRecord<?, ?> record, String name) {
        new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8)
    }

    private static int headerCount(ProducerRecord<?, ?> record, String name) {
        int count = 0
        for (def ignored : record.headers().headers(name)) {
            count++
        }
        count
    }

    private static Object defaultValue(Class<?> returnType) {
        if (returnType == boolean) {
            return false
        }
        if (returnType == byte) {
            return (byte) 0
        }
        if (returnType == short) {
            return (short) 0
        }
        if (returnType == int) {
            return 0
        }
        if (returnType == long) {
            return 0L
        }
        if (returnType == float) {
            return 0f
        }
        if (returnType == double) {
            return 0d
        }
        if (returnType == char) {
            return (char) 0
        }
        null
    }

    private static final class TestListener {
    }
}
