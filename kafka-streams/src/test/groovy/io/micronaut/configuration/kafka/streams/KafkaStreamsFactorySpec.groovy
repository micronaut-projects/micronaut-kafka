package io.micronaut.configuration.kafka.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.context.event.ApplicationEventPublisher
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.*

class KafkaStreamsFactorySpec extends Specification {

    void "set exception handler when no config is given"() {
        given:
        KafkaStreamsFactory kafkaStreamsFactory = newKafkaStreamsFactory()
        Properties props = new Properties()

        when:
        Optional<StreamsUncaughtExceptionHandler> handler = kafkaStreamsFactory.makeUncaughtExceptionHandler(props)

        then:
        handler.empty
    }

    void "set exception handler when no valid config is given"() {
        given:
        KafkaStreamsFactory kafkaStreamsFactory = newKafkaStreamsFactory()
        Properties props = ['uncaught-exception-handler': config]

        when:
        Optional<StreamsUncaughtExceptionHandler> handler = kafkaStreamsFactory.makeUncaughtExceptionHandler(props)

        then:
        handler.empty

        where:
        config << ['', ' ', 'ILLEGAL_VALUE', '!!REPLACE_THREAD!!']
    }

    @Unroll
    void "set exception handler when given config is #config"(String config) {
        given:
        KafkaStreamsFactory kafkaStreamsFactory = newKafkaStreamsFactory()
        Properties props = ['uncaught-exception-handler': config]

        when:
        Optional<StreamsUncaughtExceptionHandler> handler = kafkaStreamsFactory.makeUncaughtExceptionHandler(props)

        then:
        handler.present
        handler.get().handle(null) == expected

        where:
        config                 | expected
        'replace_thread'       | REPLACE_THREAD
        'shutdown_CLIENT'      | SHUTDOWN_CLIENT
        'SHUTDOWN_APPLICATION' | SHUTDOWN_APPLICATION
    }

    void "shutdownGracefully closes active streams and updates active task count"() {
        given:
        KafkaStreamsFactory kafkaStreamsFactory = newKafkaStreamsFactory()
        def closed = new AtomicBoolean(false)
        def stream = Mock(org.apache.kafka.streams.KafkaStreams) {
            state() >> { closed.get() ? org.apache.kafka.streams.KafkaStreams.State.NOT_RUNNING : org.apache.kafka.streams.KafkaStreams.State.RUNNING }
            close(Duration.ofSeconds(1)) >> {
                closed.set(true)
                true
            }
        }
        kafkaStreamsFactory.streams.put(stream, new ConfiguredStreamBuilder(new Properties(), "test", Duration.ofSeconds(1)))

        expect:
        kafkaStreamsFactory.reportActiveTasks().asLong == 1L

        when:
        kafkaStreamsFactory.shutdownGracefully().get()

        then:
        kafkaStreamsFactory.reportActiveTasks().asLong == 0L
    }

    void "SHUTDOWN_APPLICATION handler requests Micronaut shutdown"() {
        given:
        CountDownLatch stopCalled = new CountDownLatch(1)
        KafkaStreamsFactory kafkaStreamsFactory = new KafkaStreamsFactory(
            Stub(ApplicationEventPublisher),
            Stub(ApplicationContext) {
                isRunning() >> true
                stop() >> {
                    stopCalled.countDown()
                    null
                }
            }
        )
        Properties props = ['uncaught-exception-handler': 'SHUTDOWN_APPLICATION']

        when:
        kafkaStreamsFactory.makeUncaughtExceptionHandler(props).orElseThrow().handle(new RuntimeException("boom"))

        then:
        stopCalled.await(5, TimeUnit.SECONDS)
    }

    private KafkaStreamsFactory newKafkaStreamsFactory() {
        new KafkaStreamsFactory(
            Stub(ApplicationEventPublisher),
            Stub(ApplicationContext) {
                isRunning() >> true
            }
        )
    }
}
