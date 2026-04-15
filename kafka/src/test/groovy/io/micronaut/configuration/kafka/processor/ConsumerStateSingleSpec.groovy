package io.micronaut.configuration.kafka.processor

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification
import sun.misc.Unsafe

import java.lang.reflect.Field
import java.lang.reflect.Method
import java.lang.reflect.Proxy

class ConsumerStateSingleSpec extends Specification {

    void "reset the following partitions seeks each topic partition independently"() {
        given:
        ConsumerStateSingle state = allocateInstance(ConsumerStateSingle)
        List<TopicPartition> seeks = []
        List<Long> offsets = []
        Consumer consumer = createConsumer(seeks, offsets)
        setField(ConsumerState, state, "kafkaConsumer", consumer)
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
                if (method.name == "seek") {
                    seeks << (TopicPartition) args[0]
                    offsets << (Long) args[1]
                    return null
                }
                defaultValue(method.returnType)
            }
        ) as Consumer
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

    private static <T> T allocateInstance(Class<T> type) {
        Field field = Unsafe.class.getDeclaredField("theUnsafe")
        field.accessible = true
        Unsafe unsafe = (Unsafe) field.get(null)
        Method allocateInstance = Unsafe.class.getDeclaredMethod("allocateInstance", Class)
        allocateInstance.accessible = true
        type.cast(allocateInstance.invoke(unsafe, type))
    }

    private static void setField(Class<?> owner, Object instance, String fieldName, Object value) {
        Field field = owner.getDeclaredField(fieldName)
        field.accessible = true
        field.set(instance, value)
    }
}
