package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.Argument
import io.micronaut.core.type.ReturnType
import io.micronaut.inject.ExecutableMethod
import io.micronaut.messaging.exceptions.MessagingSystemException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification

import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletableFuture

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.LOG_AND_RESUME_AT_NEXT_RECORD

class ConsumerStateSingleSpec extends Specification {

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
            kafkaListenerAnnotation(null),
            executableMethod()
        )

        then:
        def ex = thrown(MessagingSystemException)
        ex.message.contains('LOG_AND_RESUME_AT_NEXT_RECORD')
        ex.message.contains('dlq')
    }

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

    private ConsumerStateSingle newConsumerStateSingle(KafkaConsumerProcessor kafkaConsumerProcessor, Consumer<?, ?> kafkaConsumer) {
        ConsumerInfo consumerInfo = new ConsumerInfo(
            'client',
            'group',
            OffsetStrategy.DISABLED,
            kafkaListenerAnnotation('errors-dlq'),
            executableMethod()
        )
        new ConsumerStateSingle(kafkaConsumerProcessor, consumerInfo, kafkaConsumer, new Object())
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
            { _, method, args ->
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
                    case "getReturnType":
                        return returnType
                    default:
                        return defaultValue(method.returnType)
                }
            }
        ) as ExecutableMethod<?, ?>
        AnnotationValue<KafkaListener> annotation = AnnotationValue.builder(KafkaListener).build()
        new ConsumerInfo("test-client", "test-group", OffsetStrategy.SYNC, annotation, executableMethod)
    }

    private static Object invokePrivateMethod(Object target, String name, Class[] parameterTypes, Object[] arguments) {
        def method = target.class.getDeclaredMethod(name, parameterTypes)
        method.accessible = true
        method.invoke(target, arguments)
    }

    private AnnotationValue<KafkaListener> kafkaListenerAnnotation(String dlq) {
        def errorStrategyAnnotation = AnnotationValue.builder(ErrorStrategy)
            .member('value', LOG_AND_RESUME_AT_NEXT_RECORD)
        if (dlq != null) {
            errorStrategyAnnotation.member('dlq', dlq)
        }
        AnnotationValue.builder(KafkaListener)
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
            getDeclaringType() >> TestListener
            getName() >> 'receive'
            isTrue(KafkaListener, 'batch') >> false
            hasAnnotation(_ as Class) >> false
            getValue(KafkaListener, 'pollTimeout', Duration) >> Optional.of(Duration.ofMillis(100))
            getArguments() >> Argument.ZERO_ARGUMENTS
            stringValues(_ as Class) >> ([] as String[])
            getReturnType() >> returnType
        }
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
