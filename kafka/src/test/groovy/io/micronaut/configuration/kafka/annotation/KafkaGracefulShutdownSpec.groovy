package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires

import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class KafkaGracefulShutdownSpec extends AbstractKafkaContainerSpec {

    void "graceful shutdown completes promptly with idle consumers"() {
        given: "a new application context with graceful shutdown enabled"
        def config = [
            'kafka.bootstrap.servers': bootstrapServers,
            'kafka.enabled': 'true',
            'spec.name': 'KafkaGracefulShutdownIdleSpec',
            'micronaut.lifecycle.graceful-shutdown.enabled': 'true',
            'micronaut.lifecycle.graceful-shutdown.grace-period': '30s',
            (EMBEDDED_TOPICS): ['idle-topic']
        ]
        ApplicationContext ctx = ApplicationContext.run(config)

        when: "the context is closed"
        Instant start = Instant.now()
        ctx.close()
        Duration shutdownDuration = Duration.between(start, Instant.now())

        then: "shutdown completes quickly without waiting the full grace period"
        shutdownDuration.seconds < 5
    }

    void "graceful shutdown completes in-flight message processing"() {
        given: "a consumer that takes time to process messages"
        def config = [
            'kafka.bootstrap.servers': bootstrapServers,
            'kafka.enabled': 'true',
            'spec.name': 'KafkaGracefulShutdownInFlightSpec',
            'micronaut.lifecycle.graceful-shutdown.enabled': 'true',
            'micronaut.lifecycle.graceful-shutdown.grace-period': '30s',
            (EMBEDDED_TOPICS): ['in-flight-products']
        ]
        ApplicationContext ctx = ApplicationContext.run(config)

        when: "a message is sent"
        def client = ctx.getBean(InFlightClient)
        def consumer = ctx.getBean(InFlightConsumer)
        client.send("test-product")

        then: "message starts processing"
        conditions.eventually {
            consumer.processing.get()
        }

        when: "shutdown is triggered while processing"
        Instant start = Instant.now()
        ctx.close()
        Duration shutdownDuration = Duration.between(start, Instant.now())

        then: "the message is fully processed"
        consumer.messageProcessed.get()

        and: "shutdown completes before grace period"
        shutdownDuration.seconds < 15
    }

    @Requires(property = 'spec.name', value = 'KafkaGracefulShutdownIdleSpec')
    @KafkaListener(groupId = "idle-consumer", offsetReset = EARLIEST)
    static class IdleConsumer {
        @Topic("idle-topic")
        void receive(String message) {
            // Idle consumer - receives no messages during test
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaGracefulShutdownInFlightSpec')
    @KafkaListener(groupId = "in-flight-consumer", offsetReset = EARLIEST)
    static class InFlightConsumer {
        AtomicBoolean processing = new AtomicBoolean(false)
        AtomicBoolean messageProcessed = new AtomicBoolean(false)

        @Topic("in-flight-products")
        void receive(String product) {
            processing.set(true)
            try {
                // Simulate some processing time
                sleep(2000)
                messageProcessed.set(true)
            } finally {
                processing.set(false)
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaGracefulShutdownInFlightSpec')
    @KafkaClient
    static interface InFlightClient {
        @Topic("in-flight-products")
        void send(String product)
    }
}
