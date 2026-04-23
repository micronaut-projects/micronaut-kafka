package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.ProducerRegistry
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
import io.micronaut.inject.BeanDefinition
import io.micronaut.runtime.ApplicationConfiguration
import spock.lang.Specification

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
}
