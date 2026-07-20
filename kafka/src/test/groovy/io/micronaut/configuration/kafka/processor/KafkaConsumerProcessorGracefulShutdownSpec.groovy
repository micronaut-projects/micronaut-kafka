package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.ProducerRegistry
import io.micronaut.configuration.kafka.ConsumerRecordInterceptor
import io.micronaut.configuration.kafka.TransactionalProducerRegistry
import io.micronaut.configuration.kafka.bind.ConsumerRecordBinderRegistry
import io.micronaut.configuration.kafka.bind.batch.BatchConsumerRecordsBinderRegistry
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration
import io.micronaut.configuration.kafka.event.KafkaConsumerStartedPollingEvent
import io.micronaut.configuration.kafka.event.KafkaConsumerSubscribedEvent
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.configuration.kafka.retry.ConditionalRetryBehaviourHandler
import io.micronaut.configuration.kafka.serde.SerdeRegistry
import io.micronaut.context.BeanProvider
import io.micronaut.context.BeanContext
import io.micronaut.context.event.ApplicationEventPublisher
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.Argument
import io.micronaut.core.type.ReturnType
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.inject.BeanDefinition
import io.micronaut.inject.ExecutableMethod
import io.micronaut.runtime.ApplicationConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification

import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService

class KafkaConsumerProcessorGracefulShutdownSpec extends Specification {

    void "shutdownGracefully wakes consumers and waits for their shutdown futures"() {
        given:
        KafkaConsumerProcessor processor = newKafkaConsumerProcessor()
        ConsumerState first = Mock()
        ConsumerState second = Mock()
        CompletableFuture<Void> firstFuture = new CompletableFuture<>()
        CompletableFuture<Void> secondFuture = new CompletableFuture<>()
        processor.@consumers.put('first', first)
        processor.@consumers.put('second', second)

        when:
        CompletableFuture<Void> shutdown = processor.shutdownGracefully()

        then:
        !shutdown.done
        1 * first.requestShutdown()
        1 * first.wakeUp()
        1 * first.getShutdownFuture() >> firstFuture
        1 * second.requestShutdown()
        1 * second.wakeUp()
        1 * second.getShutdownFuture() >> secondFuture

        when:
        firstFuture.complete(null)

        then:
        !shutdown.done

        when:
        secondFuture.complete(null)

        then:
        shutdown.get() == null
    }

    void "reportActiveTasks counts active consumers"() {
        given:
        KafkaConsumerProcessor processor = newKafkaConsumerProcessor()
        ConsumerState first = Stub() {
            isActive() >> true
        }
        ConsumerState second = Stub() {
            isActive() >> false
        }
        ConsumerState third = Stub() {
            isActive() >> true
        }
        processor.@consumers.put('first', first)
        processor.@consumers.put('second', second)
        processor.@consumers.put('third', third)

        when:
        OptionalLong activeTasks = processor.reportActiveTasks()

        then:
        activeTasks.present
        activeTasks.asLong == 2L
    }

    void "interceptRecord applies interceptors in order"() {
        given:
        ConsumerRecord<String, String> record = new ConsumerRecord<>('books', 1, 3L, 'key', 'value')
        KafkaConsumerProcessor processor = newKafkaConsumerProcessor([
            interceptor(10) { ConsumerRecordInterceptor.InterceptionContext<String, String> context ->
                ConsumerRecord<String, String> intercepted = context.consumerRecord()
                new ConsumerRecord<>(intercepted.topic(), intercepted.partition(), intercepted.offset(), intercepted.key(), intercepted.value() + '-late')
            },
            interceptor(-10) { ConsumerRecordInterceptor.InterceptionContext<String, String> context ->
                ConsumerRecord<String, String> intercepted = context.consumerRecord()
                new ConsumerRecord<>(intercepted.topic(), intercepted.partition(), intercepted.offset(), intercepted.key(), intercepted.value() + '-early')
            }
        ])
        ConsumerInfo consumerInfo = consumerInfo(processor.matchingInterceptors(beanDefinition('receive'), executableMethod('receive')))

        when:
        ConsumerRecord<String, String> intercepted = processor.interceptRecord(consumerInfo, record)

        then:
        intercepted.value() == 'value-early-late'
    }

    void "interceptRecords rebuilds batches with filtered records"() {
        given:
        TopicPartition partition = new TopicPartition('books', 1)
        ConsumerRecord<String, String> kept = new ConsumerRecord<>('books', 1, 3L, 'key-1', 'keep')
        ConsumerRecord<String, String> skipped = new ConsumerRecord<>('books', 1, 4L, 'key-2', 'skip')
        ConsumerRecord<String, String> transformed = new ConsumerRecord<>('books', 1, 5L, 'key-3', 'transform')
        ConsumerRecords<String, String> records = new ConsumerRecords<>([(partition): [kept, skipped, transformed]])
        KafkaConsumerProcessor processor = newKafkaConsumerProcessor([
            interceptor(0) { ConsumerRecordInterceptor.InterceptionContext<String, String> context ->
                ConsumerRecord<String, String> intercepted = context.consumerRecord()
                if (intercepted.value() == 'skip') {
                    return null
                }
                if (intercepted.value() == 'transform') {
                    return new ConsumerRecord<>(intercepted.topic(), intercepted.partition(), intercepted.offset(), intercepted.key(), 'wrapped')
                }
                return intercepted
            }
        ])
        ConsumerInfo consumerInfo = consumerInfo(processor.matchingInterceptors(beanDefinition('receive'), executableMethod('receive')))

        when:
        ConsumerRecords<String, String> intercepted = processor.interceptRecords(consumerInfo, records)

        then:
        intercepted.records(partition)*.value() == ['keep', 'wrapped']
    }

    void "interceptRecord rejects wrapped records that change record coordinates"() {
        given:
        ConsumerRecord<String, String> record = new ConsumerRecord<>('books', 1, 3L, 'key', 'value')
        KafkaConsumerProcessor processor = newKafkaConsumerProcessor([
            interceptor(0) { ConsumerRecordInterceptor.InterceptionContext<String, String> context ->
                ConsumerRecord<String, String> intercepted = context.consumerRecord()
                new ConsumerRecord<>('other-books', intercepted.partition(), intercepted.offset(), intercepted.key(), intercepted.value())
            }
        ])
        ConsumerInfo consumerInfo = consumerInfo(processor.matchingInterceptors(beanDefinition('receive'), executableMethod('receive')))

        when:
        processor.interceptRecord(consumerInfo, record)

        then:
        IllegalStateException e = thrown()
        e.message.contains('must preserve the consumed record topic, partition, and offset')
    }

    void "matchingInterceptors filters interceptors per listener method"() {
        given:
        KafkaConsumerProcessor processor = newKafkaConsumerProcessor([
            matchingInterceptor('receive') { ConsumerRecordInterceptor.InterceptionContext<String, String> context ->
                ConsumerRecord<String, String> intercepted = context.consumerRecord()
                new ConsumerRecord<>(intercepted.topic(), intercepted.partition(), intercepted.offset(), intercepted.key(), intercepted.value() + '-receive')
            },
            matchingInterceptor('other') { ConsumerRecordInterceptor.InterceptionContext<String, String> context ->
                ConsumerRecord<String, String> intercepted = context.consumerRecord()
                new ConsumerRecord<>(intercepted.topic(), intercepted.partition(), intercepted.offset(), intercepted.key(), intercepted.value() + '-other')
            }
        ])
        ConsumerRecord<String, String> record = new ConsumerRecord<>('books', 1, 3L, 'key', 'value')

        when:
        List<ConsumerRecordInterceptor<?, ?>> receiveInterceptors = processor.matchingInterceptors(beanDefinition('receive'), executableMethod('receive'))
        List<ConsumerRecordInterceptor<?, ?>> otherInterceptors = processor.matchingInterceptors(beanDefinition('other'), executableMethod('other'))
        ConsumerRecord<String, String> receiveRecord = processor.interceptRecord(consumerInfo(receiveInterceptors), record)
        ConsumerRecord<String, String> otherRecord = processor.interceptRecord(consumerInfo(otherInterceptors), record)

        then:
        receiveInterceptors.size() == 1
        otherInterceptors.size() == 1
        receiveRecord.value() == 'value-receive'
        otherRecord.value() == 'value-other'
    }

    void "interception context exposes listener and record metadata"() {
        given:
        ConsumerRecord<String, String> record = new ConsumerRecord<>('books', 2, 7L, 'key', 'value')
        ConsumerRecordInterceptor.InterceptionContext<String, String> seen = null
        KafkaConsumerProcessor processor = newKafkaConsumerProcessor([
            interceptor(0) { ConsumerRecordInterceptor.InterceptionContext<String, String> context ->
                seen = context
                context.consumerRecord()
            }
        ])
        ConsumerInfo consumerInfo = consumerInfo(processor.matchingInterceptors(beanDefinition('receive'), executableMethod('receive')))

        when:
        processor.interceptRecord(consumerInfo, record)

        then:
        seen.topic() == 'books'
        seen.partition() == 2
        seen.offset() == 7L
        seen.clientId() == 'test-client'
        seen.groupId() == 'test-group'
        seen.consumerRecord().is(record)
    }

    private KafkaConsumerProcessor newKafkaConsumerProcessor() {
        newKafkaConsumerProcessor([] as List<ConsumerRecordInterceptor<?, ?>>)
    }

    private KafkaConsumerProcessor newKafkaConsumerProcessor(List<ConsumerRecordInterceptor<?, ?>> consumerRecordInterceptors) {
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
            consumerRecordInterceptors,
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

    private static ConsumerRecordInterceptor<String, String> interceptor(int order, Closure<ConsumerRecord<String, String>> closure) {
        [
            getOrder : { -> order },
            intercept: { ConsumerRecordInterceptor.InterceptionContext<String, String> context -> closure.call(context) }
        ] as ConsumerRecordInterceptor<String, String>
    }

    private static ConsumerRecordInterceptor<String, String> matchingInterceptor(String methodName, Closure<ConsumerRecord<String, String>> closure) {
        [
            matches: { BeanDefinition<?> beanDefinition, ExecutableMethod<?, ?> method -> method.name == methodName },
            intercept: { ConsumerRecordInterceptor.InterceptionContext<String, String> context -> closure.call(context) }
        ] as ConsumerRecordInterceptor<String, String>
    }

    private BeanDefinition<?> beanDefinition(String methodName) {
        Stub(BeanDefinition) {
            getBeanType() >> TestListener
        }
    }

    private ExecutableMethod<?, ?> executableMethod(String methodName) {
        Stub(ExecutableMethod) {
            getName() >> methodName
            getDeclaringType() >> TestListener
            isTrue(_, _) >> false
            hasAnnotation(_) >> false
            getValue(KafkaListener, "pollTimeout", Duration) >> Optional.of(Duration.ofMillis(1))
            getArguments() >> new Argument[0]
            stringValues(_) >> null
            getReturnType() >> Stub(ReturnType) {
                getType() >> Void
                isAsyncOrReactive() >> false
                getFirstTypeVariable() >> Optional.empty()
            }
        }
    }

    private ConsumerInfo consumerInfo(List<ConsumerRecordInterceptor<?, ?>> consumerRecordInterceptors) {
        AnnotationValue<KafkaListener> kafkaListener = AnnotationValue.builder(KafkaListener).build()
        new ConsumerInfo("test-client", "test-group", OffsetStrategy.DISABLED, kafkaListener, new Properties(), executableMethod('receive'), consumerRecordInterceptors)
    }

    private static final class TestListener {
    }
}
