package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.KafkaScope
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.bind.ConsumerRecordBinderRegistry
import io.micronaut.configuration.kafka.bind.batch.BatchConsumerRecordsBinderRegistry
import io.micronaut.configuration.kafka.scope.KafkaCustomScope
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Executable
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.convert.ConversionService
import io.micronaut.inject.BeanDefinition
import io.micronaut.inject.ExecutableMethod
import jakarta.annotation.PreDestroy
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import spock.lang.AutoCleanup
import spock.lang.Specification

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

class KafkaScopeConsumerStateSpec extends Specification {

    @AutoCleanup
    ApplicationContext context = ApplicationContext.run(['spec.name': 'KafkaScopeConsumerStateSpec'])

    void setup() {
        InvocationScopedBean.DESTROYED.set(0)
        context.getBean(SingleScopedListener).reset()
        context.getBean(BatchScopedListener).reset()
    }

    void "single consumer state recreates the scope for each record"() {
        given:
        ConsumerStateSingle state = new ConsumerStateSingle(
            kafkaConsumerProcessor(),
            singleConsumerInfo(),
            kafkaConsumer(),
            context.getBean(SingleScopedListener)
        )
        ConsumerRecords<String, String> records = records(
            new ConsumerRecord<>('topic', 0, 0L, 'key-1', 'one'),
            new ConsumerRecord<>('topic', 0, 1L, 'key-2', 'two')
        )
        SingleScopedListener listener = context.getBean(SingleScopedListener)

        when:
        state.processRecords(records, [:])

        then:
        listener.directIds.size() == 2
        listener.directIds == listener.helperIds
        listener.directIds.toSet().size() == 2
        InvocationScopedBean.DESTROYED.get() == 2
    }

    void "batch consumer state shares a scope across the batch and recreates it for the next batch"() {
        given:
        ConsumerStateBatch state = new ConsumerStateBatch(
            kafkaConsumerProcessor(),
            batchConsumerInfo(),
            kafkaConsumer(),
            context.getBean(BatchScopedListener)
        )
        BatchScopedListener listener = context.getBean(BatchScopedListener)

        when:
        state.processRecords(records(
            new ConsumerRecord<>('topic', 0, 0L, 'key-1', 'one'),
            new ConsumerRecord<>('topic', 0, 1L, 'key-2', 'two')
        ), [:])
        state.processRecords(records(
            new ConsumerRecord<>('topic', 0, 2L, 'key-3', 'three')
        ), [:])

        then:
        listener.directIds.size() == 2
        listener.batchSizes == [2, 1]
        listener.directIds.toSet().size() == 2
        listener.helperIdsPerInvocation[0].every { it == listener.directIds[0] }
        listener.helperIdsPerInvocation[1].every { it == listener.directIds[1] }
        InvocationScopedBean.DESTROYED.get() == 2
    }

    private KafkaConsumerProcessor kafkaConsumerProcessor() {
        ConsumerRecordBinderRegistry binderRegistry = new ConsumerRecordBinderRegistry(ConversionService.SHARED)
        BatchConsumerRecordsBinderRegistry batchBinderRegistry = new BatchConsumerRecordsBinderRegistry(binderRegistry, ConversionService.SHARED)
        KafkaCustomScope kafkaScope = context.getBean(KafkaCustomScope)
        Mock(KafkaConsumerProcessor) {
            getBinderRegistry() >> binderRegistry
            getBatchBinderRegistry() >> batchBinderRegistry
            getKafkaScope() >> kafkaScope
        }
    }

    private Consumer<?, ?> kafkaConsumer() {
        Mock(Consumer) {
            subscription() >> Collections.emptySet()
            commitSync(_ as Map<TopicPartition, OffsetAndMetadata>) >> null
            commitAsync(_ as Map<TopicPartition, OffsetAndMetadata>, _) >> null
        }
    }

    private ConsumerInfo singleConsumerInfo() {
        BeanDefinition<SingleScopedListener> beanDefinition = context.getBeanDefinition(SingleScopedListener)
        ExecutableMethod<SingleScopedListener, Object> method = beanDefinition.getRequiredMethod('receive', String)
        AnnotationValue<KafkaListener> kafkaListener = beanDefinition.getAnnotation(KafkaListener)
        new ConsumerInfo('client', 'group', OffsetStrategy.DISABLED, kafkaListener, method)
    }

    private ConsumerInfo batchConsumerInfo() {
        BeanDefinition<BatchScopedListener> beanDefinition = context.getBeanDefinition(BatchScopedListener)
        ExecutableMethod<BatchScopedListener, Object> method = beanDefinition.getRequiredMethod('receive', List)
        AnnotationValue<KafkaListener> kafkaListener = beanDefinition.getAnnotation(KafkaListener)
        new ConsumerInfo('client', 'group', OffsetStrategy.DISABLED, kafkaListener, method)
    }

    private static ConsumerRecords<String, String> records(ConsumerRecord<String, String>... records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> byPartition = [:].withDefault { [] }
        records.each { record ->
            byPartition[new TopicPartition(record.topic(), record.partition())].add(record)
        }
        new ConsumerRecords<>(byPartition)
    }

    @Requires(property = 'spec.name', value = 'KafkaScopeConsumerStateSpec')
    @Singleton
    static class InvocationScopedHelper {
        @Inject InvocationScopedBean invocationScopedBean

        String currentId() {
            invocationScopedBean.currentId()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaScopeConsumerStateSpec')
    @KafkaScope
    static class InvocationScopedBean {
        static final AtomicInteger DESTROYED = new AtomicInteger()

        final String id = UUID.randomUUID().toString()

        String currentId() {
            id
        }

        @PreDestroy
        void destroy() {
            DESTROYED.incrementAndGet()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaScopeConsumerStateSpec')
    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class SingleScopedListener {
        final List<String> directIds = new CopyOnWriteArrayList<>()
        final List<String> helperIds = new CopyOnWriteArrayList<>()

        @Inject InvocationScopedBean invocationScopedBean
        @Inject InvocationScopedHelper invocationScopedHelper

        @Executable
        void receive(String value) {
            directIds.add(invocationScopedBean.currentId())
            helperIds.add(invocationScopedHelper.currentId())
        }

        void reset() {
            directIds.clear()
            helperIds.clear()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaScopeConsumerStateSpec')
    @KafkaListener(offsetReset = OffsetReset.EARLIEST, batch = true)
    static class BatchScopedListener {
        final List<String> directIds = new CopyOnWriteArrayList<>()
        final List<Integer> batchSizes = new CopyOnWriteArrayList<>()
        final List<List<String>> helperIdsPerInvocation = new CopyOnWriteArrayList<>()

        @Inject InvocationScopedBean invocationScopedBean
        @Inject InvocationScopedHelper invocationScopedHelper

        @Executable
        void receive(List<String> values) {
            directIds.add(invocationScopedBean.currentId())
            batchSizes.add(values.size())
            helperIdsPerInvocation.add(values.collect { invocationScopedHelper.currentId() })
        }

        void reset() {
            directIds.clear()
            batchSizes.clear()
            helperIdsPerInvocation.clear()
        }
    }
}
