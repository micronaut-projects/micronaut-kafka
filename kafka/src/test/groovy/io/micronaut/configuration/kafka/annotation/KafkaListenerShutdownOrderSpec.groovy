package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.ShutdownEvent
import io.micronaut.core.order.Ordered
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
 *
 * <p>Two probes listen to the shutdown event to observe the order: one right after graceful shutdown
 * (which has the highest precedence), and one right after the consumers are stopped.</p>
 */
class KafkaListenerShutdownOrderSpec extends AbstractKafkaContainerSpec {

    void "consumers are stopped before the beans their listeners use are destroyed"() {
        given:
        ApplicationContext ctx = runContext('shutdown-order-default', [:])
        ShutdownOrderListener listener = ctx.getBean(ShutdownOrderListener)
        ShutdownOrderResource resource = ctx.getBean(ShutdownOrderResource)
        AfterConsumersStoppedProbe afterConsumersStopped = ctx.getBean(AfterConsumersStoppedProbe)

        when: "the context is stopped while a record is being processed"
        CompletableFuture<Void> stop = stopWhileProcessing(ctx, 'shutdown-order-default', listener)

        then: "the listener's dependency is not destroyed while the record is being processed"
        !resource.destroyed.await(2, TimeUnit.SECONDS)

        when: "the record finishes processing"
        listener.release.countDown()
        stop.get(30, TimeUnit.SECONDS)

        then: "it was processed against a live dependency, which was destroyed afterwards"
        afterConsumersStopped.recordFinished.get()
        listener.processedWithLiveResource.get()
        resource.destroyed.count == 0

        cleanup:
        close(ctx, listener)
    }

    void "graceful shutdown drains the consumers within the grace period"() {
        given:
        ApplicationContext ctx = runContext('shutdown-order-graceful', [
            'micronaut.lifecycle.graceful-shutdown.enabled'     : 'true',
            'micronaut.lifecycle.graceful-shutdown.grace-period': '30s'
        ])
        ShutdownOrderListener listener = ctx.getBean(ShutdownOrderListener)
        ShutdownOrderResource resource = ctx.getBean(ShutdownOrderResource)
        AfterGracefulShutdownProbe afterGracefulShutdown = ctx.getBean(AfterGracefulShutdownProbe)

        when: "the context is stopped while a record is being processed"
        CompletableFuture<Void> stop = stopWhileProcessing(ctx, 'shutdown-order-graceful', listener)

        then: "graceful shutdown waits for the record, so shutdown goes no further"
        !resource.destroyed.await(2, TimeUnit.SECONDS)
        afterGracefulShutdown.ran.count == 1

        when: "the record finishes processing within the grace period"
        listener.release.countDown()
        stop.get(30, TimeUnit.SECONDS)

        then: "graceful shutdown drained the consumer before the rest of the shutdown"
        afterGracefulShutdown.recordFinished.get()
        listener.processedWithLiveResource.get()
        resource.destroyed.count == 0

        cleanup:
        close(ctx, listener)
    }

    void "consumers still processing when the grace period expires are stopped before beans are destroyed"() {
        given:
        ApplicationContext ctx = runContext('shutdown-order-grace-expired', [
            'micronaut.lifecycle.graceful-shutdown.enabled'     : 'true',
            'micronaut.lifecycle.graceful-shutdown.grace-period': '1s'
        ])
        ShutdownOrderListener listener = ctx.getBean(ShutdownOrderListener)
        ShutdownOrderResource resource = ctx.getBean(ShutdownOrderResource)
        AfterGracefulShutdownProbe afterGracefulShutdown = ctx.getBean(AfterGracefulShutdownProbe)
        AfterConsumersStoppedProbe afterConsumersStopped = ctx.getBean(AfterConsumersStoppedProbe)

        when: "the context is stopped while a record is being processed"
        CompletableFuture<Void> stop = stopWhileProcessing(ctx, 'shutdown-order-grace-expired', listener)

        then: "the grace period expires with the record still being processed"
        afterGracefulShutdown.ran.await(15, TimeUnit.SECONDS)
        !afterGracefulShutdown.recordFinished.get()

        and: "the listener's dependency is still not destroyed"
        !resource.destroyed.await(2, TimeUnit.SECONDS)

        when: "the record finishes processing"
        listener.release.countDown()
        stop.get(30, TimeUnit.SECONDS)

        then: "the consumer was stopped after the record, and before the dependency was destroyed"
        afterConsumersStopped.recordFinished.get()
        listener.processedWithLiveResource.get()
        resource.destroyed.count == 0

        cleanup:
        close(ctx, listener)
    }

    private ApplicationContext runContext(String topic, Map<String, Object> config) {
        ApplicationContext.run([
            'kafka.bootstrap.servers': bootstrapServers,
            'spec.name'              : 'KafkaListenerShutdownOrder',
            'shutdown-order.topic'   : topic,
            (EMBEDDED_TOPICS)        : [topic]
        ] + config)
    }

    private CompletableFuture<Void> stopWhileProcessing(ApplicationContext ctx, String topic, ShutdownOrderListener listener) {
        ctx.getBean(ShutdownOrderClient).send(topic, "record")
        conditions.eventually {
            assert listener.started.count == 0
        }
        CompletableFuture.runAsync { ctx.stop() }
    }

    private static void close(ApplicationContext ctx, ShutdownOrderListener listener) {
        listener?.release?.countDown()
        if (ctx?.running) {
            ctx.stop()
        }
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
        final CountDownLatch finished = new CountDownLatch(1)
        final AtomicBoolean processedWithLiveResource = new AtomicBoolean()

        ShutdownOrderListener(ShutdownOrderResource resource) {
            this.resource = resource
        }

        @Topic('${shutdown-order.topic}')
        void receive(String record) {
            started.countDown()
            release.await(30, TimeUnit.SECONDS)
            processedWithLiveResource.set(resource.live)
            finished.countDown()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerShutdownOrder')
    @KafkaClient
    static interface ShutdownOrderClient {
        void send(@Topic String topic, String record)
    }

    static abstract class ShutdownProbe implements ApplicationEventListener<ShutdownEvent>, Ordered {
        final ShutdownOrderListener listener
        final CountDownLatch ran = new CountDownLatch(1)
        final AtomicBoolean recordFinished = new AtomicBoolean()

        ShutdownProbe(ShutdownOrderListener listener) {
            this.listener = listener
        }

        @Override
        void onApplicationEvent(ShutdownEvent event) {
            recordFinished.set(listener.finished.count == 0)
            ran.countDown()
        }
    }

    /**
     * Runs after graceful shutdown, which has the highest precedence, and before the consumers are stopped.
     */
    @Requires(property = 'spec.name', value = 'KafkaListenerShutdownOrder')
    @Singleton
    static class AfterGracefulShutdownProbe extends ShutdownProbe {
        AfterGracefulShutdownProbe(ShutdownOrderListener listener) {
            super(listener)
        }

        @Override
        int getOrder() {
            Ordered.HIGHEST_PRECEDENCE + 1
        }
    }

    /**
     * Runs right after the consumers are stopped, at {@code KafkaConsumerShutdownListener.ORDER}.
     */
    @Requires(property = 'spec.name', value = 'KafkaListenerShutdownOrder')
    @Singleton
    static class AfterConsumersStoppedProbe extends ShutdownProbe {
        AfterConsumersStoppedProbe(ShutdownOrderListener listener) {
            super(listener)
        }

        @Override
        int getOrder() {
            Ordered.HIGHEST_PRECEDENCE + 101
        }
    }
}
