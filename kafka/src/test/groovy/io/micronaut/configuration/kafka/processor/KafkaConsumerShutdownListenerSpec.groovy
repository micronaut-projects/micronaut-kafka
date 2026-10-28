package io.micronaut.configuration.kafka.processor

import io.micronaut.context.ApplicationContext
import io.micronaut.context.BeanContext
import io.micronaut.context.BeanRegistration
import io.micronaut.context.event.ShutdownEvent
import spock.lang.Specification

class KafkaConsumerShutdownListenerSpec extends Specification {

    void "stops the consumers of the active consumer processors on shutdown"() {
        given:
        KafkaConsumerProcessor processor = Mock()
        BeanRegistration<KafkaConsumerProcessor> registration = Stub() {
            bean() >> processor
        }
        BeanContext beanContext = Mock()
        KafkaConsumerShutdownListener listener = new KafkaConsumerShutdownListener(beanContext)

        when:
        listener.onApplicationEvent(new ShutdownEvent(beanContext))

        then: "the consumers are stopped, but stay registered until the processor is destroyed"
        1 * beanContext.getActiveBeanRegistrations(KafkaConsumerProcessor) >> [registration]
        1 * processor.stopConsumers()
        0 * processor.close()
        0 * beanContext._
    }

    void "does not create a consumer processor on shutdown"() {
        given:
        BeanContext beanContext = Mock()
        KafkaConsumerShutdownListener listener = new KafkaConsumerShutdownListener(beanContext)

        when:
        listener.onApplicationEvent(new ShutdownEvent(beanContext))

        then:
        1 * beanContext.getActiveBeanRegistrations(KafkaConsumerProcessor) >> []
        0 * beanContext._
    }

    void "a processor that fails to stop its consumers does not stop the others from stopping"() {
        given:
        KafkaConsumerProcessor failing = Mock() {
            stopConsumers() >> { throw new IllegalStateException("boom") }
        }
        KafkaConsumerProcessor other = Mock()
        BeanContext beanContext = Stub() {
            getActiveBeanRegistrations(KafkaConsumerProcessor) >> [
                Stub(BeanRegistration) { bean() >> failing },
                Stub(BeanRegistration) { bean() >> other }
            ]
        }

        when:
        new KafkaConsumerShutdownListener(beanContext).onApplicationEvent(new ShutdownEvent(beanContext))

        then:
        noExceptionThrown()
        1 * other.stopConsumers()
    }

    void "is not created when Kafka is #description"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(config)

        expect:
        !ctx.containsBean(KafkaConsumerShutdownListener)

        cleanup:
        ctx.close()

        where:
        description      | config
        'disabled'       | ['kafka.bootstrap.servers': 'localhost:9092', 'kafka.enabled': 'false']
        'not configured' | [:]
    }
}
