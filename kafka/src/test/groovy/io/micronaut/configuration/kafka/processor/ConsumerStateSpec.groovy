package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.BeanDefinition
import io.micronaut.inject.ExecutableMethod
import io.micronaut.configuration.kafka.annotation.KafkaListener
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification

import java.time.Duration
import java.lang.reflect.Field
import java.lang.reflect.Method
import java.lang.reflect.Proxy

class ConsumerStateSpec extends Specification {

    void "close does not spin indefinitely after a successful poll cycle"() {
        given:
        ConsumerState state = new TestConsumerState(createConsumerInfo(), createConsumer())
        setField(state, 'pollingStarted', true)

        when:
        invokeRefreshAssignmentsPollAndProcessRecords(state)
        Thread closeThread = new Thread(state.&close)
        closeThread.start()
        closeThread.join(1000)

        then:
        !closeThread.alive
        getClosedState(state) == ConsumerCloseState.POLLING

        cleanup:
        if (closeThread?.alive) {
            setField(state, 'closedState', ConsumerCloseState.CLOSED)
            closeThread.join(1000)
        }
    }

    private static ConsumerInfo createConsumerInfo() {
        try (ApplicationContext context = ApplicationContext.run()) {
            BeanDefinition<ConsumerStateSpecListener> beanDefinition = context.getBeanDefinition(ConsumerStateSpecListener)
            ExecutableMethod<ConsumerStateSpecListener, Object> method = beanDefinition.getRequiredMethod('receive', String)
            AnnotationValue<KafkaListener> kafkaListener = beanDefinition.getAnnotation(KafkaListener)
            return new ConsumerInfo('client', 'group', OffsetStrategy.ASYNC_PER_RECORD, kafkaListener, method)
        }
    }

    @SuppressWarnings('unchecked')
    private static Consumer<Object, Object> createConsumer() {
        TopicPartition topicPartition = new TopicPartition('topic', 0)
        Proxy.newProxyInstance(
            ConsumerStateSpec.classLoader,
            [Consumer] as Class<?>[],
            { proxy, method, args ->
                switch (method.name) {
                    case 'subscription':
                        return ['topic'] as Set
                    case 'assignment':
                        return [topicPartition] as Set
                    case 'paused':
                        return [] as Set
                    case 'close':
                    case 'wakeup':
                    case 'pause':
                    case 'resume':
                    case 'commitSync':
                    case 'commitAsync':
                    case 'seek':
                        return null
                    default:
                        return null
                }
            }
        ) as Consumer<Object, Object>
    }

    private static void invokeRefreshAssignmentsPollAndProcessRecords(ConsumerState state) {
        invokeMethod(state, 'refreshAssignmentsPollAndProcessRecords')
    }

    private static ConsumerCloseState getClosedState(ConsumerState state) {
        getField(state, 'closedState') as ConsumerCloseState
    }

    private static Object getField(Object target, String name) {
        Field field = ConsumerState.getDeclaredField(name)
        field.accessible = true
        field.get(target)
    }

    private static void setField(Object target, String name, Object value) {
        Field field = ConsumerState.getDeclaredField(name)
        field.accessible = true
        field.set(target, value)
    }

    private static Object invokeMethod(Object target, String name) {
        Method method = ConsumerState.getDeclaredMethod(name)
        method.accessible = true
        method.invoke(target)
    }

    private static final class TestConsumerState extends ConsumerState {

        TestConsumerState(ConsumerInfo info, Consumer<?, ?> consumer) {
            super(null, info, consumer, new ConsumerStateSpecListener())
        }

        @Override
        protected ConsumerRecords<?, ?> pollRecords(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
            ConsumerRecords.empty()
        }

        @Override
        protected void processRecords(ConsumerRecords<?, ?> consumerRecords, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        }

        @Override
        protected Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
            [:]
        }

        @Override
        protected Duration getCloseTimeout() {
            Duration.ofMillis(200)
        }
    }
}
