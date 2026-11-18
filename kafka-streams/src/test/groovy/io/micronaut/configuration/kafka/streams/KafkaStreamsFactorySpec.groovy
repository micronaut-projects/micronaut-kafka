package io.micronaut.configuration.kafka.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.context.event.ApplicationEventPublisher
import io.micronaut.inject.qualifiers.Qualifiers
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.CloseOptions
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
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
        kafkaStreamsFactory.getStreams().put(stream, new ConfiguredStreamBuilder(new Properties(), "test", Duration.ofSeconds(1)))

        expect:
        kafkaStreamsFactory.reportActiveTasks().asLong == 1L

        when:
        kafkaStreamsFactory.shutdownGracefully().get()

        then:
        kafkaStreamsFactory.reportActiveTasks().asLong == 0L
    }

    void "named streams bind shutdown options without passing them to Kafka Streams"() {
        given:
        ApplicationContext context = ApplicationContext.builder([
            'spec.name': 'GlobalKTableOnlySpec',
            'kafka.test.initializer.enabled': false,
            'kafka.bootstrap.servers': 'localhost:9092',
            'kafka.streams.global-table-only.application.id': 'factory-spec-global-table',
            'kafka.streams.global-table-only.start-kafka-streams': false,
            'kafka.streams.application-security-factors.leave-group-on-close': true,
            'kafka.streams.application-security-factors.close-timeout': '3s',
            'kafka.streams.another-stream.leave-group-on-close': false,
            'kafka.streams.another-stream.close-timeout': '5s',
            'kafka.streams.omitted-stream.close-timeout': '7s'
        ]).bootstrapEnvironment(false).start()

        when:
        def configurations = ['application-security-factors', 'another-stream', 'omitted-stream'].collectEntries { name ->
            [(name): context.getBean(KafkaStreamsConfiguration, Qualifiers.byName(name))]
        }
        def builders = configurations.keySet().collectEntries { name ->
            [(name): context.getBean(ConfiguredStreamBuilder, Qualifiers.byName(name))]
        }

        then:
        configurations['application-security-factors'].leaveGroupOnClose
        !configurations['another-stream'].leaveGroupOnClose
        !configurations['omitted-stream'].leaveGroupOnClose
        builders['application-security-factors'].leaveGroupOnClose
        !builders['another-stream'].leaveGroupOnClose
        !builders['omitted-stream'].leaveGroupOnClose
        builders['application-security-factors'].closeTimeout == Duration.ofSeconds(3)
        builders['another-stream'].closeTimeout == Duration.ofSeconds(5)
        builders['omitted-stream'].closeTimeout == Duration.ofSeconds(7)
        configurations.values().every { !it.config.containsKey('leave-group-on-close') }
        builders.values().every { !it.configuration.containsKey('leave-group-on-close') }

        cleanup:
        context?.close()
    }

    void "graceful shutdown uses the per-stream close overload and timeout"() {
        given:
        KafkaStreamsFactory factory = newKafkaStreamsFactory()
        def calls = new ConcurrentLinkedQueue()
        def optedIn = Stub(KafkaStreams) {
            close(_ as CloseOptions) >> { CloseOptions options ->
                calls.add(['opted-in', options.@leaveGroup, options.@timeout])
                true
            }
            close(_ as Duration) >> { calls.add('unexpected opted-in duration close'); true }
        }
        def optedOut = Stub(KafkaStreams) {
            close(_ as Duration) >> { Duration timeout ->
                calls.add(['opted-out', timeout])
                true
            }
            close(_ as CloseOptions) >> { calls.add('unexpected opted-out options close'); true }
        }
        def omitted = Stub(KafkaStreams) {
            close(_ as Duration) >> { Duration timeout ->
                calls.add(['omitted', timeout])
                true
            }
            close(_ as CloseOptions) >> { calls.add('unexpected omitted options close'); true }
        }
        factory.streams.put(optedIn, new ConfiguredStreamBuilder(new Properties(), 'opted-in', Duration.ofSeconds(3), true))
        factory.streams.put(optedOut, new ConfiguredStreamBuilder(new Properties(), 'opted-out', Duration.ofSeconds(5), false))
        factory.streams.put(omitted, new ConfiguredStreamBuilder(new Properties(), 'omitted', Duration.ofSeconds(7)))

        when:
        factory.shutdownGracefully().get()

        then:
        calls.size() == 3
        calls.contains(['opted-in', true, Duration.ofSeconds(3)])
        calls.contains(['opted-out', Duration.ofSeconds(5)])
        calls.contains(['omitted', Duration.ofSeconds(7)])
    }

    void "close invokes graceful shutdown once and clears the registered streams"() {
        given:
        KafkaStreamsFactory factory = newKafkaStreamsFactory()
        def calls = new ConcurrentLinkedQueue()
        def stream = Stub(KafkaStreams) {
            close(_ as CloseOptions) >> { CloseOptions options ->
                calls.add([options.@leaveGroup, options.@timeout])
                true
            }
            close(_ as Duration) >> { calls.add('unexpected duration close'); true }
        }
        factory.streams.put(stream, new ConfiguredStreamBuilder(new Properties(), 'opted-in', Duration.ofSeconds(3), true))

        when:
        factory.close()

        then:
        calls.toList() == [[true, Duration.ofSeconds(3)]]
        factory.streams.isEmpty()

        when:
        factory.close()

        then:
        calls.toList() == [[true, Duration.ofSeconds(3)]]
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
