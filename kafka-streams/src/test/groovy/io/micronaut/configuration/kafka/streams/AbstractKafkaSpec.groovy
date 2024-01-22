package io.micronaut.configuration.kafka.streams

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

abstract class AbstractKafkaSpec extends Specification {

    static log = LoggerFactory.getLogger(getClass())

    protected final PollingConditions conditions = new PollingConditions(timeout: conditionsTimeout, delay: 1)

    protected int getConditionsTimeout() {
        60
    }

    protected Map<String, Object> getConfiguration() {
        ['spec.name': getClass().simpleName]
    }
}
