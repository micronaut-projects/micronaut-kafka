package io.micronaut.configuration.kafka.event

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.KafkaConsumer
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import static org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchStates.FETCHING

@Property(name = "spec.name", value = "KafkaConsumerEventSpec")
@MicronautTest(startApplication = false)
class KafkaConsumerEventSpec extends Specification {

    @Inject
    KafkaConsumerSubscribedEventListener subscribedEventListener

    @Inject
    KafkaConsumerStartedPollingEventListener startedPollingEvent

    void "listen to kafka consumer subscribed events"() {
        expect: "the event is emitted and consumed by the event listener"
        new PollingConditions(timeout: 10, delay: 1).eventually {
            subscribedEventListener.received instanceof KafkaConsumerSubscribedEvent
        }
        and: "the kafka consumer is subscribed to the expected topic"
        subscribedEventListener.consumer.subscriptions.subscription == ['my-nifty-topic'] as Set
    }

    void "listen to kafka consumer started polling events"() {
        expect: "the event is emitted and consumed by the event listener"
        new PollingConditions(timeout: 10, delay: 1).eventually {
            startedPollingEvent.received instanceof KafkaConsumerStartedPollingEvent
        }
        and: "the kafka consumer starts fetching records"
        new PollingConditions(timeout: 10, delay: 1).eventually {
            subscribedEventListener.consumer.subscriptions.assignment.partitionStateValues()[0].fetchState == FETCHING
        }
    }

    @KafkaListener(clientId = "my-nifty-kafka-consumer")
    @Requires(property = "spec.name", value = "KafkaConsumerEventSpec")
    static class MyKafkaConsumer {
        @Topic("my-nifty-topic")
        void consume(String event) {}
    }

    static class AbstractKafkaConsumerEventListener<T extends AbstractKafkaApplicationEvent> implements ApplicationEventListener<T> {
        AbstractKafkaApplicationEvent received
        KafkaConsumer consumer

        @Override
        void onApplicationEvent(T event) {
            // Skip consumers unrelated to this test
            if ((event.source as KafkaConsumer).clientId.startsWith('my-nifty-kafka-consumer')) {
                if (received != null) throw new RuntimeException("Expecting one event only")
                received = event
                consumer = event.source
            }
        }
    }

    @Singleton
    @Requires(property = "spec.name", value = "KafkaConsumerEventSpec")
    static class KafkaConsumerSubscribedEventListener extends AbstractKafkaConsumerEventListener<KafkaConsumerSubscribedEvent> { }

    @Singleton
    @Requires(property = "spec.name", value = "KafkaConsumerEventSpec")
    static class KafkaConsumerStartedPollingEventListener extends AbstractKafkaConsumerEventListener<KafkaConsumerStartedPollingEvent> { }
}
