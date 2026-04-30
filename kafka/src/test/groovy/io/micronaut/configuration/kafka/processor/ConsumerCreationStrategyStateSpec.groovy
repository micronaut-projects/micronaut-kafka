package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.bind.ConsumerRecordBinderRegistry
import io.micronaut.configuration.kafka.bind.batch.BatchConsumerRecordsBinderRegistry
import io.micronaut.core.convert.ConversionService
import io.micronaut.inject.BeanDefinitionReference
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import spock.lang.Shared
import spock.lang.Specification

import java.util.Properties

class ConsumerCreationStrategyStateSpec extends Specification {

    @Shared
    ConsumerRecordBinderRegistry binderRegistry = new ConsumerRecordBinderRegistry(ConversionService.SHARED)

    @Shared
    BatchConsumerRecordsBinderRegistry batchBinderRegistry = new BatchConsumerRecordsBinderRegistry(binderRegistry, ConversionService.SHARED)

    void 'single state routes each topic to the matching method'() {
        given:
        def listener = new TestPerClassMultiTopicListener()
        def processor = Stub(KafkaConsumerProcessor) {
            getBinderRegistry() >> binderRegistry
        }
        def kafkaConsumer = Stub(Consumer) {
            subscription() >> ([] as Set)
        }
        def state = new ConsumerStateSingle(processor, consumerInfo(TestPerClassMultiTopicListener), kafkaConsumer, listener)
        def records = consumerRecords(
            foo: ['one'],
            foo2: ['two'],
            bar: ['three']
        )

        when:
        state.processRecords(records, [:] as Map<TopicPartition, OffsetAndMetadata>)

        then:
        listener.fooValues == ['one', 'two']
        listener.barValues == ['three']
    }

    void 'batch state routes each topic to the matching method'() {
        given:
        def listener = new TestPerClassMultiTopicBatchListener()
        def processor = Stub(KafkaConsumerProcessor) {
            getBatchBinderRegistry() >> batchBinderRegistry
        }
        def kafkaConsumer = Stub(Consumer) {
            subscription() >> ([] as Set)
        }
        def state = new ConsumerStateBatch(processor, consumerInfo(TestPerClassMultiTopicBatchListener), kafkaConsumer, listener)
        def records = consumerRecords(
            foo: ['one', 'two'],
            foo2: ['three', 'four'],
            bar: ['five', 'six']
        )

        when:
        state.processRecords(records, [:] as Map<TopicPartition, OffsetAndMetadata>)

        then:
        listener.fooValues == ['one', 'two', 'three', 'four']
        listener.barValues == ['five', 'six']
    }

    void 'batch state keeps single-method multi-topic listeners as one callback per poll'() {
        given:
        def listener = new TestBatchMultiTopicListener()
        def processor = Stub(KafkaConsumerProcessor) {
            getBatchBinderRegistry() >> batchBinderRegistry
        }
        def kafkaConsumer = Stub(Consumer) {
            subscription() >> ([] as Set)
        }
        def state = new ConsumerStateBatch(processor, consumerInfo(TestBatchMultiTopicListener), kafkaConsumer, listener)
        def records = consumerRecords(
            foo: ['one'],
            bar: ['two']
        )

        when:
        state.processRecords(records, [:] as Map<TopicPartition, OffsetAndMetadata>)

        then:
        listener.invocations == 1
        listener.values == ['one', 'two']
    }

    private ConsumerInfo consumerInfo(Class<?> beanType) {
        def definitionType = Class.forName("${beanType.packageName}.\$${beanType.simpleName}\$Definition")
        def beanDefinition = ((BeanDefinitionReference<?>) definitionType.getDeclaredConstructor().newInstance()).load()
        def methods = beanDefinition.executableMethods.findAll {
            !it.getDeclaredAnnotationValuesByType(Topic).isEmpty()
        }
        new ConsumerInfo(
            'test-client',
            'test-group',
            OffsetStrategy.AUTO,
            methods[0].getAnnotation(KafkaListener),
            new Properties(),
            methods
        )
    }

    private static ConsumerRecords<String, String> consumerRecords(Map<String, List<String>> valuesByTopic) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = [:]
        valuesByTopic.each { topic, values ->
            def topicPartition = new TopicPartition(topic, 0)
            List<ConsumerRecord<String, String>> topicRecords = []
            for (int index = 0; index < values.size(); index++) {
                topicRecords.add(new ConsumerRecord<>(topic, 0, index as long, null, values.get(index)))
            }
            records[topicPartition] = topicRecords
        }
        new ConsumerRecords<>(records)
    }
}
