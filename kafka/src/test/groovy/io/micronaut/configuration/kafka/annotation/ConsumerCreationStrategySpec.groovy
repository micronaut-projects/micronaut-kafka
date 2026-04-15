package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageBody

import java.util.concurrent.CopyOnWriteArrayList

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST

class ConsumerCreationStrategySpec extends AbstractKafkaContainerSpec {

    static final String FOO_TOPIC = 'ConsumerCreationStrategySpec-foo'
    static final String BAR_TOPIC = 'ConsumerCreationStrategySpec-bar'
    static final String BATCH_FOO_TOPIC = 'ConsumerCreationStrategySpec-batch-foo'
    static final String BATCH_BAR_TOPIC = 'ConsumerCreationStrategySpec-batch-bar'

    void 'test PER_CLASS creates one consumer and routes records by topic'() {
        given:
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        MultiTopicClient client = context.getBean(MultiTopicClient)
        MultiTopicListener listener = context.getBean(MultiTopicListener)

        when:
        client.sendFoo('one')
        client.sendFoo('two')
        client.sendBar('three')

        then:
        conditions.eventually {
            listener.fooValues == ['one', 'two']
            listener.barValues == ['three']
        }

        and:
        registry.consumerIds.findAll { it.startsWith('multi-topic-listener') }.size() == 1
        registry.getConsumerSubscription('multi-topic-listener') == [FOO_TOPIC, BAR_TOPIC] as Set
    }

    void 'test PER_CLASS batch listener routes each topic to the matching method'() {
        given:
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        MultiTopicBatchClient client = context.getBean(MultiTopicBatchClient)
        MultiTopicBatchListener listener = context.getBean(MultiTopicBatchListener)

        when:
        client.sendFoo(['one', 'two'])
        client.sendBar(['three', 'four'])

        then:
        conditions.eventually {
            listener.fooValues == ['one', 'two']
            listener.barValues == ['three', 'four']
        }

        and:
        registry.consumerIds.findAll { it.startsWith('multi-topic-batch-listener') }.size() == 1
        registry.getConsumerSubscription('multi-topic-batch-listener') == [BATCH_FOO_TOPIC, BATCH_BAR_TOPIC] as Set
    }

    @Requires(property = 'spec.name', value = 'ConsumerCreationStrategySpec')
    @KafkaClient
    static interface MultiTopicClient {

        @Topic(FOO_TOPIC)
        void sendFoo(@MessageBody String body)

        @Topic(BAR_TOPIC)
        void sendBar(@MessageBody String body)
    }

    @Requires(property = 'spec.name', value = 'ConsumerCreationStrategySpec')
    @KafkaListener(
        clientId = 'multi-topic-listener',
        consumerCreationStrategy = ConsumerCreationStrategy.PER_CLASS,
        offsetReset = EARLIEST
    )
    static class MultiTopicListener {
        List<String> fooValues = new CopyOnWriteArrayList<>()
        List<String> barValues = new CopyOnWriteArrayList<>()

        @Topic(FOO_TOPIC)
        void receiveFoo(String body) {
            fooValues.add(body)
        }

        @Topic(BAR_TOPIC)
        void receiveBar(String body) {
            barValues.add(body)
        }
    }

    @Requires(property = 'spec.name', value = 'ConsumerCreationStrategySpec')
    @KafkaClient(batch = true)
    static interface MultiTopicBatchClient {

        @Topic(BATCH_FOO_TOPIC)
        void sendFoo(List<String> body)

        @Topic(BATCH_BAR_TOPIC)
        void sendBar(List<String> body)
    }

    @Requires(property = 'spec.name', value = 'ConsumerCreationStrategySpec')
    @KafkaListener(
        batch = true,
        clientId = 'multi-topic-batch-listener',
        consumerCreationStrategy = ConsumerCreationStrategy.PER_CLASS,
        offsetReset = EARLIEST
    )
    static class MultiTopicBatchListener {
        List<String> fooValues = new CopyOnWriteArrayList<>()
        List<String> barValues = new CopyOnWriteArrayList<>()

        @Topic(BATCH_FOO_TOPIC)
        void receiveFoo(List<String> body) {
            fooValues.addAll(body)
        }

        @Topic(BATCH_BAR_TOPIC)
        void receiveBar(List<String> body) {
            barValues.addAll(body)
        }
    }
}
