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
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Duration
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
                    headerValue(record, 'micronaut-kafka-exception-message') == 'boom' &&
                    headerValue(record, 'micronaut-kafka-original-topic') == 'source-topic' &&
                    headerValue(record, 'micronaut-kafka-original-partition') == '2' &&
                    headerValue(record, 'micronaut-kafka-original-offset') == '7'
        }) >> CompletableFuture.completedFuture(null)
        1 * kafkaConsumerProcessor.handleException(_, _)
    }

    private ConsumerStateSingle newConsumerStateSingle(KafkaConsumerProcessor kafkaConsumerProcessor, Consumer<?, ?> kafkaConsumer) {
        ConsumerInfo consumerInfo = new ConsumerInfo(
            'client',
            'group',
            OffsetStrategy.DISABLED,
            kafkaListenerAnnotation(),
            executableMethod()
        )
        new ConsumerStateSingle(kafkaConsumerProcessor, consumerInfo, kafkaConsumer, new Object())
    }

    private static Object invokePrivateMethod(Object target, String name, Class[] parameterTypes, Object[] arguments) {
        def method = target.class.getDeclaredMethod(name, parameterTypes)
        method.accessible = true
        method.invoke(target, arguments)
    }

    private AnnotationValue<KafkaListener> kafkaListenerAnnotation() {
        AnnotationValue.builder(KafkaListener)
            .member('errorStrategy', AnnotationValue.builder(ErrorStrategy)
                .member('value', LOG_AND_RESUME_AT_NEXT_RECORD)
                .member('dlq', 'errors-dlq')
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

    private static final class TestListener {
    }
}
