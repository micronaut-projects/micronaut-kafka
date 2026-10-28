package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

/**
 * The listener's dependency has no dependency relationship with the consumer processor, and its bean
 * name sorts before it, so without stopping the consumers on shutdown it would be destroyed while a
 * record is still being processed.
 */
class KafkaListenerShutdownOrderSpec extends AbstractKafkaContainerSpec {

    void "consumers are stopped before the beans their listeners use are destroyed (graceful shutdown #graceful)"() {
        given:
        String topic = "shutdown-order-$graceful"
        ApplicationContext ctx = ApplicationContext.run([
            'kafka.bootstrap.servers'                      : bootstrapServers,
            'spec.name'                                    : 'KafkaListenerShutdownOrder',
            'shutdown-order.topic'                         : topic,
            'micronaut.lifecycle.graceful-shutdown.enabled': graceful.toString(),
            (EMBEDDED_TOPICS)                              : [topic]
        ])
        ShutdownOrderListener listener = ctx.getBean(ShutdownOrderListener)
        ShutdownOrderResource resource = ctx.getBean(ShutdownOrderResource)

        when: "a record is being processed"
        ctx.getBean(ShutdownOrderClient).send(topic, "record")

        then:
        conditions.eventually {
            listener.started.count == 0
        }

        when: "the context is stopped while the record is being processed"
        CompletableFuture<Void> stop = CompletableFuture.runAsync { ctx.stop() }

        then: "the listener's dependency is not destroyed while the record is being processed"
        !resource.destroyed.await(2, TimeUnit.SECONDS)

        when: "the record finishes processing"
        listener.release.countDown()
        stop.get(30, TimeUnit.SECONDS)

        then: "it was processed against a live dependency, which was destroyed afterwards"
        listener.processedWithLiveResource.get()
        resource.destroyed.count == 0

        cleanup:
        listener?.release?.countDown()
        if (ctx?.running) {
            ctx.stop()
        }

        where:
        graceful << [false, true]
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerShutdownOrder')
    @Singleton
    static class ShutdownOrderResource {
        final CountDownLatch destroyed = new CountDownLatch(1)

        boolean isLive() {
            destroyed.count == 1
        }

        @PreDestroy
        void close() {
            destroyed.countDown()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerShutdownOrder')
    @KafkaListener(groupId = "shutdown-order", offsetReset = EARLIEST)
    static class ShutdownOrderListener {
        final ShutdownOrderResource resource
        final CountDownLatch started = new CountDownLatch(1)
        final CountDownLatch release = new CountDownLatch(1)
        final AtomicBoolean processedWithLiveResource = new AtomicBoolean()

        ShutdownOrderListener(ShutdownOrderResource resource) {
            this.resource = resource
        }

        @Topic('${shutdown-order.topic}')
        void receive(String record) {
            started.countDown()
            release.await(30, TimeUnit.SECONDS)
            processedWithLiveResource.set(resource.live)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerShutdownOrder')
    @KafkaClient
    static interface ShutdownOrderClient {
        void send(@Topic String topic, String record)
    }
}
