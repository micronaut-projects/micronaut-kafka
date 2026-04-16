package io.micronaut.configuration.kafka.exceptions

import io.micronaut.configuration.kafka.config.DefaultKafkaListenerExceptionHandlerConfigurationProperties
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.CommitFailedException
import org.slf4j.Logger
import spock.lang.Specification
import spock.lang.Unroll

class DefaultKafkaListenerExceptionHandlerLoggingSpec extends Specification {

    @Unroll
    void "logs commit failures at #expectedLevel when cooperativeSticky=#cooperativeSticky"() {
        given:
        Logger logger = Mock()
        DefaultKafkaListenerExceptionHandler handler = new DefaultKafkaListenerExceptionHandler(new DefaultKafkaListenerExceptionHandlerConfigurationProperties(), logger)
        CommitFailedException cause = new CommitFailedException('rebalance in progress')
        KafkaListenerException exception = new KafkaListenerException(
            'commit failed',
            cause,
            'listener',
            Stub(Consumer),
            null,
            null,
            cooperativeSticky
        )

        when:
        handler.handle(exception)

        then:
        if (cooperativeSticky) {
            1 * logger.warn('Kafka consumer [{}] produced error: {}', {
                it.length == 3 && it[0] == 'listener' && it[1] == 'rebalance in progress' && it[2].is(cause)
            } as Object[])
            0 * logger.error(_, _ as Object[])
        } else {
            1 * logger.error('Kafka consumer [{}] produced error: {}', {
                it.length == 3 && it[0] == 'listener' && it[1] == 'rebalance in progress' && it[2].is(cause)
            } as Object[])
            0 * logger.warn(_, _ as Object[])
        }

        where:
        cooperativeSticky || expectedLevel
        true              || 'WARN'
        false             || 'ERROR'
    }
}
