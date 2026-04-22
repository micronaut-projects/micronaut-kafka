package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.Argument
import io.micronaut.core.type.ReturnType
import io.micronaut.inject.ExecutableMethod
import io.micronaut.messaging.annotation.SendTo
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification

import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.time.Duration
import java.util.Optional

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
}
