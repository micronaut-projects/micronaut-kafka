package io.micronaut.configuration.kafka.scope

import io.micronaut.configuration.kafka.annotation.KafkaScope
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import jakarta.annotation.PreDestroy
import jakarta.inject.Inject
import jakarta.inject.Singleton
import spock.lang.AutoCleanup
import spock.lang.Specification

import java.util.concurrent.atomic.AtomicInteger

class KafkaScopeSpec extends Specification {

    @AutoCleanup
    ApplicationContext context = ApplicationContext.run(['spec.name': 'KafkaScopeSpec'])

    void setup() {
        InvocationScopedBean.DESTROYED.set(0)
    }

    void "kafka scoped beans resolve within an active scope and are recreated for the next scope"() {
        given:
        KafkaCustomScope scope = context.getBean(KafkaCustomScope)
        ScopedConsumer consumer = context.getBean(ScopedConsumer)

        when:
        String directId
        String helperId
        try (def ignored = scope.open()) {
            directId = consumer.directId()
            helperId = consumer.helperId()
        }

        and:
        String nextId
        try (def ignored = scope.open()) {
            nextId = consumer.directId()
        }

        then:
        directId == helperId
        directId != nextId
        InvocationScopedBean.DESTROYED.get() == 2
    }

    void "kafka scoped bean access fails without an active scope"() {
        given:
        ScopedConsumer consumer = context.getBean(ScopedConsumer)

        when:
        consumer.directId()

        then:
        IllegalStateException e = thrown()
        e.message.contains('No active Kafka scope')
    }

    @Requires(property = 'spec.name', value = 'KafkaScopeSpec')
    @Singleton
    static class ScopedConsumer {
        @Inject InvocationScopedBean invocationScopedBean
        @Inject ScopedHelper scopedHelper

        String directId() {
            invocationScopedBean.currentId()
        }

        String helperId() {
            scopedHelper.currentId()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaScopeSpec')
    @Singleton
    static class ScopedHelper {
        @Inject InvocationScopedBean invocationScopedBean

        String currentId() {
            invocationScopedBean.currentId()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaScopeSpec')
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
}
