package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.Argument
import io.micronaut.core.type.ReturnType
import io.micronaut.inject.ExecutableMethod
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification

import java.time.Duration
import java.util.Properties

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
                [ConsumerRecords, Map, Throwable] as Class[],
                [consumerRecords, currentOffsets, new RuntimeException('boom')] as Object[]
        ) as boolean

        then:
        shouldRetry
        1 * kafkaConsumer.seek(batchPartition, 10L)
        0 * kafkaConsumer.seek(existingPartition, _)
        0 * kafkaConsumerProcessor.handleException(_, _)
    }

    private ConsumerStateBatch newConsumerStateBatch() {
        newConsumerStateBatch(Mock(KafkaConsumerProcessor), Mock(Consumer) {
            subscription() >> Collections.emptySet()
        })
    }

    private ConsumerStateBatch newConsumerStateBatch(KafkaConsumerProcessor kafkaConsumerProcessor, Consumer<?, ?> kafkaConsumer) {
        ConsumerInfo consumerInfo = new ConsumerInfo(
                'client',
                'group',
                OffsetStrategy.DISABLED,
                kafkaListenerAnnotation(),
                new Properties(),
                executableMethod()
        )
        new ConsumerStateBatch(kafkaConsumerProcessor, consumerInfo, kafkaConsumer, new Object())
    }

    private static Object invokePrivateMethod(Object target, String name, Class[] parameterTypes, Object[] arguments) {
        def method = target.class.getDeclaredMethod(name, parameterTypes)
        method.accessible = true
        method.invoke(target, arguments)
    }

    private AnnotationValue<KafkaListener> kafkaListenerAnnotation() {
        AnnotationValue.builder(KafkaListener)
                .member('batch', true)
                .member('errorStrategy', AnnotationValue.builder(ErrorStrategy)
                        .member('value', RETRY_ON_ERROR)
                        .member('retryCount', 3)
                        .build())
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
}
