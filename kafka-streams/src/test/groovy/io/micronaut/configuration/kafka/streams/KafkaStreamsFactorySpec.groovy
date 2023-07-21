package io.micronaut.configuration.kafka.streams

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import spock.lang.Unroll

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.*

class KafkaStreamsFactorySpec extends AbstractTestContainersSpec {

    void "set exception handler when no config is given"() {
        given:
        KafkaStreamsFactory kafkaStreamsFactory = context.getBean(KafkaStreamsFactory)
        KafkaStreams kafkaStreams = Mock()
        Properties props = new Properties()

        when:
        def handler = kafkaStreamsFactory.setUncaughtExceptionHandler(props, kafkaStreams)

        then:
        handler.empty
        0 * _
    }

    void "set exception handler when no valid config is given"() {
        given:
        KafkaStreamsFactory kafkaStreamsFactory = context.getBean(KafkaStreamsFactory)
        KafkaStreams kafkaStreams = Mock()
        Properties props = ['uncaught-exception-handler': config]

        when:
        def handler = kafkaStreamsFactory.setUncaughtExceptionHandler(props, kafkaStreams)

        then:
        handler.empty
        0 * _

        where:
        config << ['', ' ', 'ILLEGAL_VALUE', '!!REPLACE_THREAD!!']
    }

    @Unroll
    void "set exception handler when given config is #config"(String config) {
        given:
        KafkaStreamsFactory kafkaStreamsFactory = context.getBean(KafkaStreamsFactory)
        KafkaStreams kafkaStreams = Mock()
        Properties props = ['uncaught-exception-handler': config]

        when:
        def handler = kafkaStreamsFactory.setUncaughtExceptionHandler(props, kafkaStreams)

        then:
        handler.present
        handler.get().handle(null) == expected
        1 * kafkaStreams.setUncaughtExceptionHandler(_ as StreamsUncaughtExceptionHandler)
        0 * _

        where:
        config                 | expected
        'replace_thread'       | REPLACE_THREAD
        'shutdown_CLIENT'      | SHUTDOWN_CLIENT
        'SHUTDOWN_APPLICATION' | SHUTDOWN_APPLICATION
    }
}
