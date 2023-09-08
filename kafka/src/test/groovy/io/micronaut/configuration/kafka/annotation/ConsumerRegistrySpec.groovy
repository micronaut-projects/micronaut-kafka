package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageBody
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentSkipListSet

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST

class ConsumerRegistrySpec extends AbstractKafkaContainerSpec {

    @Override
    protected Map<String, Object> getConfiguration() {
        return super.getConfiguration() + ['micrometer.metrics.enabled' : true, 'endpoints.metrics.sensitive': false]
    }

    void 'test consumer registry'() {
        given:
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        BicycleClient client = context.getBean(BicycleClient)
        BicycleListener listener = context.getBean(BicycleListener)

        when:
        Consumer consumer = registry.getConsumer('bicycle-client')

        then:
        consumer

        when:
        Set<String> consumerIds = registry.consumerIds

        then:
        consumerIds.contains 'bicycle-client'

        and:
        consumerIds.contains 'bike-client'
        consumerIds.count { it.startsWith('bike-client-') } == 2

        when:
        Set<String> subscription = registry.getConsumerSubscription('bicycle-client')

        then:
        subscription
        subscription.size() == 1
        subscription[0] == 'bicycles'

        when:
        client.send 'Raleigh', 'Professional'

        then:
        conditions.eventually {
            listener.bicycles.size() == 1
            listener.bicycles[0] == 'Professional'
        }

        when:
        Set<TopicPartition> topicPartitions = registry.getConsumerAssignment('bicycle-client')

        then:
        topicPartitions
        topicPartitions.size() == 1
        topicPartitions[0].topic() == 'bicycles'
        topicPartitions[0].partition() == 0
    }

    @Requires(property = 'spec.name', value = 'ConsumerRegistrySpec')
    @KafkaClient
    static interface BicycleClient {
        @Topic('bicycles')
        void send(@KafkaKey String make, @MessageBody String model)
    }

    @Requires(property = 'spec.name', value = 'ConsumerRegistrySpec')
    @KafkaListener(clientId = 'bicycle-client', offsetReset = EARLIEST)
    static class BicycleListener {

        Set<String> bicycles = new ConcurrentSkipListSet<>()

        @Topic('bicycles')
        void receive(@MessageBody String model) {
            bicycles << model
        }
    }

    @Requires(property = 'spec.name', value = 'ConsumerRegistrySpec')
    @KafkaListener(clientId = 'bike-client')
    static class BikeListener1 {
        @Topic('bikes')
        void receive(@MessageBody String model) { }
    }

    @Requires(property = 'spec.name', value = 'ConsumerRegistrySpec')
    @KafkaListener(clientId = 'bike-client')
    static class BikeListener2 {
        @Topic('bikes')
        void receive(@MessageBody String model) { }
    }

    @Requires(property = 'spec.name', value = 'ConsumerRegistrySpec')
    @KafkaListener(clientId = 'bike-client')
    static class BikeListener3 {
        @Topic('bikes')
        void receive(@MessageBody String model) { }
    }
}
