package io.micronaut.configuration.kafka.event

import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.micronaut.testcontainers.kafka.Kafka
import jakarta.inject.Inject
import jakarta.inject.Singleton
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@Property(name = "spec.name", value = "KafkaConsumerEventSpec")
@MicronautTest(startApplication = false)
class KafkaConsumerEventSpec extends Specification implements TestPropertyProvider {

    private static final String CLIENT_ID = "my-nifty-kafka-consumer"
    private static final String TOPIC = "my-nifty-topic"

    @Inject
    KafkaConsumerSubscribedEventListener subscribedEventListener

    @Inject
    KafkaConsumerStartedPollingEventListener startedPollingEvent

    @Inject
    ConsumerRegistry consumerRegistry

    @Override
    Map<String, String> getProperties() {
        Map<String, String> properties = Kafka.getProperties()
        return properties
    }

    void "listen to kafka consumer subscribed events"() {
        expect: "the event is emitted and consumed by the event listener"
        new PollingConditions(timeout: 10, delay: 1).eventually {
            subscribedEventListener.received instanceof KafkaConsumerSubscribedEvent
        }
        and: "the kafka consumer is subscribed to the expected topic"
        subscribedEventListener.subscription == [TOPIC] as Set
    }

    void "listen to kafka consumer started polling events"() {
        expect: "the event is emitted and consumed by the event listener"
        new PollingConditions(timeout: 10, delay: 1).eventually {
            startedPollingEvent.received instanceof KafkaConsumerStartedPollingEvent
        }
        and: "the kafka consumer has a public assignment for the expected topic"
        new PollingConditions(timeout: 10, delay: 1).eventually {
            def assignment = consumerRegistry.getConsumerAssignment(CLIENT_ID)
            assignment.size() == 1
            assignment.first().topic() == TOPIC
        }
    }

    @KafkaListener(clientId = CLIENT_ID)
    @Requires(property = "spec.name", value = "KafkaConsumerEventSpec")
    static class MyKafkaConsumer {
        @Topic(TOPIC)
        void consume(String event) {}
    }

    static class AbstractKafkaConsumerEventListener<T extends AbstractKafkaApplicationEvent> implements ApplicationEventListener<T> {
        AbstractKafkaApplicationEvent received
        Set<String> subscription = Collections.emptySet()

        @Override
        void onApplicationEvent(T event) {
            def currentSubscription = event.source.subscription()
            if (currentSubscription.contains(TOPIC)) {
                if (received != null) throw new RuntimeException("Expecting one event only")
                received = event
                subscription = Set.copyOf(currentSubscription)
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
