package io.micronaut.configuration.kafka.exceptions

import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.slf4j.Logger
import spock.lang.Specification
import spock.lang.Unroll

class OffsetCommitExceptionLoggerSpec extends Specification {

    @Unroll
    void "detects cooperative sticky assignor for #description"() {
        expect:
        OffsetCommitExceptionLogger.isCooperativeStickyAssignor(value) == expected

        where:
        description                 | value                                                                                                 || expected
        'fully qualified class'     | CooperativeStickyAssignor.class.name                                                                    || true
        'simple class name'         | CooperativeStickyAssignor.class.simpleName                                                              || true
        'class object'              | CooperativeStickyAssignor.class                                                                         || true
        'first configured strategy' | "${CooperativeStickyAssignor.class.name}, org.apache.kafka.clients.consumer.RangeAssignor"            || true
        'first strategy in list'    | [CooperativeStickyAssignor.class.name, 'org.apache.kafka.clients.consumer.RangeAssignor']             || true
        'first strategy in array'   | [CooperativeStickyAssignor.class.name, 'org.apache.kafka.clients.consumer.RangeAssignor'] as Object[] || true
        'later comma separated strategy'| "org.apache.kafka.clients.consumer.RangeAssignor, ${CooperativeStickyAssignor.class.name}"        || false
        'later strategy in list'    | ['org.apache.kafka.clients.consumer.RangeAssignor', CooperativeStickyAssignor.class.name]             || false
        'later strategy in array'   | ['org.apache.kafka.clients.consumer.RangeAssignor', CooperativeStickyAssignor.class.name] as Object[] || false
        'different assignor'        | 'org.apache.kafka.clients.consumer.RangeAssignor'                                                      || false
        'missing value'             | null                                                                                                   || false
    }

    @Unroll
    void "logs commit failures at #expectedLevel for cooperativeSticky=#cooperativeSticky"() {
        given:
        Logger logger = Mock()
        Exception exception = cooperativeSticky ? new CommitFailedException('boom') : new IllegalStateException('boom')

        when:
        OffsetCommitExceptionLogger.log(logger, cooperativeSticky, 'Commit failed for offsets [{}]: {}',
            exception, 'offsets', 'boom')

        then:
        if (cooperativeSticky) {
            1 * logger.isWarnEnabled() >> true
            1 * logger.warn('Commit failed for offsets [{}]: {}', {
                it.length == 3 && it[0] == 'offsets' && it[1] == 'boom' && it[2].is(exception)
            } as Object[])
            0 * logger.error(_, _ as Object[])
        } else {
            1 * logger.isErrorEnabled() >> true
            1 * logger.error('Commit failed for offsets [{}]: {}', {
                it.length == 3 && it[0] == 'offsets' && it[1] == 'boom' && it[2].is(exception)
            } as Object[])
            0 * logger.warn(_, _ as Object[])
        }

        where:
        cooperativeSticky || expectedLevel
        true              || 'WARN'
        false             || 'ERROR'
    }

    def "non-commit failures stay at error even for cooperative sticky assignors"() {
        given:
        Logger logger = Mock()
        IllegalStateException exception = new IllegalStateException('boom')

        when:
        OffsetCommitExceptionLogger.log(logger, true, 'Commit failed for offsets [{}]: {}',
            exception, 'offsets', 'boom')

        then:
        1 * logger.isErrorEnabled() >> true
        1 * logger.error('Commit failed for offsets [{}]: {}', {
            it.length == 3 && it[0] == 'offsets' && it[1] == 'boom' && it[2].is(exception)
        } as Object[])
        0 * logger.warn(_, _ as Object[])
    }

    def "does not log commit failures when target level is disabled"() {
        given:
        Logger logger = Mock()
        IllegalStateException exception = new IllegalStateException('boom')

        when:
        OffsetCommitExceptionLogger.log(logger, true, 'Commit failed', exception, 'offsets')
        OffsetCommitExceptionLogger.log(logger, false, 'Commit failed', exception, 'offsets')

        then:
        2 * logger.isErrorEnabled() >> false
        0 * logger.warn(_, _ as Object[])
        0 * logger.error(_, _ as Object[])
    }
}
