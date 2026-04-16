package io.micronaut.configuration.kafka.exceptions

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
        'class instance'            | CooperativeStickyAssignor.class                                                                         || true
        'comma separated strategies'| "org.apache.kafka.clients.consumer.RangeAssignor, ${CooperativeStickyAssignor.class.name}"            || true
        'list of strategies'        | ['org.apache.kafka.clients.consumer.RangeAssignor', CooperativeStickyAssignor.class.name]             || true
        'array of strategies'       | ['org.apache.kafka.clients.consumer.RangeAssignor', CooperativeStickyAssignor.class.name] as Object[] || true
        'different assignor'        | 'org.apache.kafka.clients.consumer.RangeAssignor'                                                      || false
        'missing value'             | null                                                                                                   || false
    }

    @Unroll
    void "logs commit failures at #expectedLevel for cooperativeSticky=#cooperativeSticky"() {
        given:
        Logger logger = Mock()
        IllegalStateException exception = new IllegalStateException('boom')

        when:
        OffsetCommitExceptionLogger.log(logger, cooperativeSticky, 'Commit failed for offsets [{}]: {}',
            exception, 'offsets', 'boom')

        then:
        if (cooperativeSticky) {
            1 * logger.warn('Commit failed for offsets [{}]: {}', {
                it.length == 3 && it[0] == 'offsets' && it[1] == 'boom' && it[2].is(exception)
            } as Object[])
            0 * logger.error(_, _ as Object[])
        } else {
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
}
