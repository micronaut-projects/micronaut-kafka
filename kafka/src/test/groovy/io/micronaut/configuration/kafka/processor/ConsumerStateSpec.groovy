package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.BeanDefinition
import io.micronaut.inject.ExecutableMethod
import io.micronaut.configuration.kafka.annotation.KafkaListener
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification

import java.time.Duration
import java.lang.reflect.Field
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.util.Properties
import java.util.concurrent.CountDownLatch

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

    void "a poll cycle does not ask the consumer for paused partitions when none were paused"() {
        given:
        List<String> calls = []
        ConsumerState state = new TestConsumerState(createConsumerInfo(), createConsumer(calls, ['paused']))
        // the started polling event needs the processor, which these states do not have
        setField(state, 'pollingStarted', true)

        when:
        invokeRefreshAssignmentsPollAndProcessRecords(state)

        then:
        noExceptionThrown()
        !calls.contains('paused')
    }

    void "pausing and resuming partitions still uses the consumer"() {
        given:
        List<String> calls = []
        TopicPartition topicPartition = new TopicPartition('topic', 0)
        Set<TopicPartition> pausedOnConsumer = [] as Set
        ConsumerState state = new TestConsumerState(createConsumerInfo(), createConsumer(calls, [], pausedOnConsumer))
        // the started polling event needs the processor, which these states do not have
        setField(state, 'pollingStarted', true)

        when:
        state.pause([topicPartition])
        invokeRefreshAssignmentsPollAndProcessRecords(state)

        then:
        calls.contains('pause')
        pausedOnConsumer == [topicPartition] as Set
        state.isPaused([topicPartition])

        when:
        state.resume([topicPartition])
        invokeRefreshAssignmentsPollAndProcessRecords(state)

        then:
        calls.contains('resume')
        pausedOnConsumer.empty
        !state.isPaused([topicPartition])
    }

    void "waking up tolerates a consumer that does not support it"() {
        given:
        List<String> calls = []
        ConsumerState state = new TestConsumerState(createConsumerInfo(), createConsumer(calls, ['wakeup']))
        // a consumer that does not start automatically waits for this latch
        CountDownLatch startupLatch = new CountDownLatch(1)
        setField(state, 'startupLatch', startupLatch)

        when:
        state.wakeUp()

        then:
        noExceptionThrown()
        calls.contains('wakeup')
        startupLatch.count == 0
    }

    void "partitions the application paused on the consumer stay paused while others are paused through the state"() {
        given:
        TopicPartition p0 = new TopicPartition('topic', 0)
        TopicPartition p1 = new TopicPartition('topic', 1)
        Set<TopicPartition> pausedOnConsumer = [] as Set
        ConsumerState state = new TestConsumerState(createConsumerInfo(),
            createConsumer([], [], pausedOnConsumer, [p0, p1] as Set))
        setField(state, 'pollingStarted', true)

        when:
        state.pause([p0])
        invokeRefreshAssignmentsPollAndProcessRecords(state)
        // the application pauses another partition on the consumer itself
        pausedOnConsumer << p1
        invokeRefreshAssignmentsPollAndProcessRecords(state)

        then:
        pausedOnConsumer == [p0, p1] as Set

        when:
        state.resume([p0])
        invokeRefreshAssignmentsPollAndProcessRecords(state)

        then:
        pausedOnConsumer == [p1] as Set
    }

    void "a paused partition that is revoked and then resumed is forgotten without resuming it"() {
        given:
        List<String> calls = []
        TopicPartition p0 = new TopicPartition('topic', 0)
        TopicPartition p1 = new TopicPartition('topic', 1)
        Set<TopicPartition> pausedOnConsumer = [] as Set
        Set<TopicPartition> assigned = [p0, p1] as Set
        TestConsumerState state = new TestConsumerState(createConsumerInfo(),
            createConsumer(calls, [], pausedOnConsumer, assigned))
        setField(state, 'pollingStarted', true)

        when:
        state.pause([p0])
        invokeRefreshAssignmentsPollAndProcessRecords(state)
        // p0 is revoked, which also clears its paused state on the consumer, which no longer accepts resuming it
        assigned.remove(p0)
        pausedOnConsumer.remove(p0)
        state.resume([p0])
        invokeRefreshAssignmentsPollAndProcessRecords(state)

        then:
        state.handled.empty
        state.polls == 2
        !calls.contains('resume')
        (getField(state, 'pausedTopicPartitions') as Set).empty

        when:
        calls.clear()
        invokeRefreshAssignmentsPollAndProcessRecords(state)

        then:
        !calls.contains('paused')
    }

    void "a consumer that does not support pausing keeps polling"() {
        given:
        List<String> calls = []
        TopicPartition topicPartition = new TopicPartition('topic', 0)
        TestConsumerState state = new TestConsumerState(createConsumerInfo(),
            createConsumer(calls, ['pause', 'paused', 'resume']))
        setField(state, 'pollingStarted', true)

        when:
        state.pause([topicPartition])
        5.times { invokeRefreshAssignmentsPollAndProcessRecords(state) }

        then:
        state.handled.empty
        state.polls == 5
        calls.count('pause') == 1
        !calls.contains('paused')
        !calls.contains('resume')
        !state.isPaused([topicPartition])
    }

    @SuppressWarnings('unchecked')
    private static Consumer<Object, Object> createConsumer(List<String> calls, List<String> unsupported,
                                                           Set<TopicPartition> paused = [] as Set,
                                                           Set<TopicPartition> assigned = [new TopicPartition('topic', 0)] as Set) {
        Proxy.newProxyInstance(
            ConsumerStateSpec.classLoader,
            [Consumer] as Class<?>[],
            { proxy, method, args ->
                calls << method.name
                if (method.name in unsupported) {
                    throw new UnsupportedOperationException(method.name + ' is not supported')
                }
                switch (method.name) {
                    case 'subscription':
                        return ['topic'] as Set
                    case 'assignment':
                        return assigned
                    case 'paused':
                        return new HashSet<>(paused)
                    case 'pause':
                        rejectUnassigned(assigned, args[0] as Collection<TopicPartition>)
                        paused.addAll(args[0] as Collection<TopicPartition>)
                        return null
                    case 'resume':
                        rejectUnassigned(assigned, args[0] as Collection<TopicPartition>)
                        paused.removeAll(args[0] as Collection<TopicPartition>)
                        return null
                    default:
                        return null
                }
            }
        ) as Consumer<Object, Object>
    }

    // like the real consumer, which only pauses and resumes partitions that are assigned to it
    private static void rejectUnassigned(Set<TopicPartition> assigned, Collection<TopicPartition> partitions) {
        TopicPartition unassigned = partitions.find { !assigned.contains(it) }
        if (unassigned != null) {
            throw new IllegalStateException('No current assignment for partition ' + unassigned)
        }
    }

    private static ConsumerInfo createConsumerInfo() {
        try (ApplicationContext context = ApplicationContext.run(['spec.name': 'ConsumerStateSpec'])) {
            BeanDefinition<ConsumerStateSpecListener> beanDefinition = context.getBeanDefinition(ConsumerStateSpecListener)
            ExecutableMethod<ConsumerStateSpecListener, Object> method = beanDefinition.getRequiredMethod('receive', String)
            AnnotationValue<KafkaListener> kafkaListener = beanDefinition.getAnnotation(KafkaListener)
            return new ConsumerInfo('client', 'group', OffsetStrategy.ASYNC_PER_RECORD, kafkaListener, new Properties(), method, [])
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

        int polls
        final List<Throwable> handled = []

        TestConsumerState(ConsumerInfo info, Consumer<?, ?> consumer) {
            super(null, info, consumer, new ConsumerStateSpecListener())
        }

        @Override
        protected ConsumerRecords<?, ?> pollRecords(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
            polls++
            ConsumerRecords.empty()
        }

        @Override
        protected void processRecords(ConsumerRecords<?, ?> consumerRecords, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        }

        @Override
        protected void handleException(Throwable e, ConsumerRecords<?, ?> consumerRecords, ConsumerRecord<?, ?> consumerRecord) {
            handled << e
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
