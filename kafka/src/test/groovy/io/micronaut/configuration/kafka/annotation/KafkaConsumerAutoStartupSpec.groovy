package io.micronaut.configuration.kafka.annotation

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Introspected
import spock.lang.Shared

import java.util.stream.IntStream

class KafkaConsumerAutoStartupSpec extends AbstractEmbeddedServerSpec {

    @Shared
    TestListener listener
    @Shared
    TestProducer producer
    @Shared
    ConsumerRegistry consumerRegistry

    void setupSpec() {
        listener = context.getBean(TestListener)
        producer = context.getBean(TestProducer)
        consumerRegistry = context.getBean(ConsumerRegistry)
    }

    def "should start consumer paused"() {
        when:
            IntStream.range(0, 5).forEach { i -> producer.send(UUID.randomUUID(), new TestEvent(i)) }
        then:
            Thread.sleep(10000)
            listener.events.size() == 0
            consumerRegistry.resume("xyz")
            conditions.eventually {
                listener.events.size() == 5
            }
    }

    @Introspected
    @EqualsAndHashCode
    @ToString
    static class TestEvent {

        int count

        TestEvent() {
        }

        TestEvent(int count) {
            this.count = count
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST, autoStartup = false, clientId = "xyz")
    @Requires(property = 'spec.name', value = 'KafkaConsumerAutoStartupSpec')
    static class TestListener {

        Set<TestEvent> events = new HashSet<>()

        @Topic("as-test-topic")
        void receive(@KafkaKey UUID key, TestEvent event) {
            events.add(event)
        }

    }

    @Requires(property = 'spec.name', value = 'KafkaConsumerAutoStartupSpec')
    @KafkaClient
    static interface TestProducer {

        @Topic("as-test-topic")
        void send(@KafkaKey UUID key, TestEvent event);
    }

}
