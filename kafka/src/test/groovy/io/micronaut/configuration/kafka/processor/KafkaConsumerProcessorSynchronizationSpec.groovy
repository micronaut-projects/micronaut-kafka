package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.ProducerRegistry
import io.micronaut.configuration.kafka.TransactionalProducerRegistry
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.bind.ConsumerRecordBinderRegistry
import io.micronaut.configuration.kafka.bind.batch.BatchConsumerRecordsBinderRegistry
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration
import io.micronaut.configuration.kafka.event.KafkaConsumerStartedPollingEvent
import io.micronaut.configuration.kafka.event.KafkaConsumerSubscribedEvent
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.configuration.kafka.retry.ConditionalRetryBehaviourHandler
import io.micronaut.configuration.kafka.serde.SerdeRegistry
import io.micronaut.context.BeanContext
import io.micronaut.context.BeanProvider
import io.micronaut.context.event.ApplicationEventPublisher
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.Argument
import io.micronaut.core.type.ReturnType
import io.micronaut.inject.BeanDefinition
import io.micronaut.inject.ExecutableMethod
import io.micronaut.runtime.ApplicationConfiguration
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import spock.lang.Specification

import java.lang.reflect.Proxy
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class KafkaConsumerProcessorSynchronizationSpec extends Specification {

    void "getConsumer serializes access with the poll loop"() {
        given:
        KafkaConsumerProcessor processor = newKafkaConsumerProcessor()
        BlockingConsumerHandler handler = new BlockingConsumerHandler()
        Consumer<?, ?> kafkaConsumer = handler.consumer()
        ConsumerStateSingle state = new ConsumerStateSingle(processor, consumerInfo(), kafkaConsumer, new Object())
        processor.@consumers.put('test-client', state)
        Thread pollThread = new Thread(state.&threadPollLoop, 'kafka-consumer-poll')

        when:
        pollThread.start()

        then:
        handler.awaitPollStarted()

        when:
        CompletableFuture<Set<TopicPartition>> assignmentFuture = CompletableFuture.supplyAsync {
            processor.getConsumer('test-client').assignment()
        }
        sleep(200)

        then:
        !assignmentFuture.isDone()

        when:
        state.requestShutdown()
        state.wakeUp()

        then:
        assignmentFuture.get(5, TimeUnit.SECONDS) == BlockingConsumerHandler.ASSIGNMENT
        pollThread.join(5000)
        !pollThread.isAlive()
        handler.maxConcurrentAccess.get() == 1
    }

    private KafkaConsumerProcessor newKafkaConsumerProcessor() {
        BeanContext beanContext = Stub() {
            getBeanDefinitions(_) >> ([] as Collection<BeanDefinition<?>>)
        }
        new KafkaConsumerProcessor(
            Stub(ExecutorService),
            Stub(ApplicationConfiguration),
            Stub(BeanProvider<KafkaConsumerGroupManager>),
            beanContext,
            Stub(AbstractKafkaConsumerConfiguration),
            Stub(ConsumerRecordBinderRegistry),
            Stub(BatchConsumerRecordsBinderRegistry),
            Stub(SerdeRegistry),
            Stub(ProducerRegistry),
            Stub(KafkaListenerExceptionHandler),
            Stub(ScheduledExecutorService),
            Stub(TransactionalProducerRegistry),
            Stub(ApplicationEventPublisher<KafkaConsumerStartedPollingEvent>),
            Stub(ApplicationEventPublisher<KafkaConsumerSubscribedEvent>),
            Stub(ConditionalRetryBehaviourHandler)
        )
    }

    private ConsumerInfo consumerInfo() {
        ReturnType<?> returnType = Stub() {
            getType() >> void
            isAsyncOrReactive() >> false
            getFirstTypeVariable() >> Optional.empty()
        }
        ExecutableMethod<?, ?> method = Stub() {
            getDeclaringType() >> KafkaConsumerProcessorSynchronizationSpec
            getName() >> 'receive'
            isTrue(KafkaListener, 'batch') >> false
            hasAnnotation(_ as Class) >> false
            getValue(KafkaListener, 'pollTimeout', Duration) >> Optional.of(Duration.ofSeconds(5))
            getArguments() >> Argument.ZERO_ARGUMENTS
            stringValues(_ as Class) >> ([] as String[])
            getReturnType() >> returnType
        }
        new ConsumerInfo(
            'test-client',
            'test-group',
            OffsetStrategy.DISABLED,
            AnnotationValue.builder(KafkaListener).build(),
            new Properties(),
            method
        )
    }

    private static final class BlockingConsumerHandler {
        static final Set<TopicPartition> ASSIGNMENT = Collections.singleton(new TopicPartition('topic', 0))

        final CountDownLatch pollStarted = new CountDownLatch(1)
        final CountDownLatch releasePoll = new CountDownLatch(1)
        final AtomicBoolean wakeupRequested = new AtomicBoolean()
        final AtomicInteger activeCalls = new AtomicInteger()
        final AtomicInteger maxConcurrentAccess = new AtomicInteger()

        Consumer<?, ?> consumer() {
            Proxy.newProxyInstance(
                BlockingConsumerHandler.classLoader,
                [Consumer] as Class<?>[],
                { _, method, args ->
                    if (method.name == 'wakeup') {
                        wakeupRequested.set(true)
                        releasePoll.countDown()
                        return null
                    }
                    int current = activeCalls.incrementAndGet()
                    maxConcurrentAccess.accumulateAndGet(current, Math::max)
                    if (current > 1) {
                        activeCalls.decrementAndGet()
                        throw new ConcurrentModificationException('Concurrent KafkaConsumer access detected')
                    }
                    try {
                        switch (method.name) {
                            case 'subscription':
                                return Collections.emptySet()
                            case 'assignment':
                                return ASSIGNMENT
                            case 'poll':
                                pollStarted.countDown()
                                releasePoll.await(5, TimeUnit.SECONDS)
                                if (wakeupRequested.get()) {
                                    throw new WakeupException()
                                }
                                return ConsumerRecords.empty()
                            case 'commitSync':
                            case 'close':
                                return null
                            default:
                                return defaultValue(method.returnType)
                        }
                    } finally {
                        activeCalls.decrementAndGet()
                    }
                }
            ) as Consumer<?, ?>
        }

        void awaitPollStarted() {
            assert pollStarted.await(5, TimeUnit.SECONDS)
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
}
