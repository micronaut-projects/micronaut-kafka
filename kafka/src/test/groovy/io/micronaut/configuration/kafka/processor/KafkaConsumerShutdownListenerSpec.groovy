package io.micronaut.configuration.kafka.processor

import io.micronaut.context.BeanContext
import io.micronaut.context.BeanRegistration
import io.micronaut.context.event.ShutdownEvent
import io.micronaut.core.order.Ordered
import spock.lang.Specification

class KafkaConsumerShutdownListenerSpec extends Specification {

    void "closes the active consumer processors on shutdown"() {
        given:
        KafkaConsumerProcessor processor = Mock()
        BeanRegistration<KafkaConsumerProcessor> registration = Stub() {
            bean() >> processor
        }
        BeanContext beanContext = Mock()
        KafkaConsumerShutdownListener listener = new KafkaConsumerShutdownListener(beanContext)

        when:
        listener.onApplicationEvent(new ShutdownEvent(beanContext))

        then:
        1 * beanContext.getActiveBeanRegistrations(KafkaConsumerProcessor) >> [registration]
        1 * processor.close()
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

    void "a processor that fails to close does not stop the others from closing"() {
        given:
        KafkaConsumerProcessor failing = Mock() {
            close() >> { throw new IllegalStateException("boom") }
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
        1 * other.close()
    }

    void "runs after graceful shutdown"() {
        expect: "GracefulShutdownListener has the highest precedence"
        new KafkaConsumerShutdownListener(Stub(BeanContext)).order > Ordered.HIGHEST_PRECEDENCE
    }
}
