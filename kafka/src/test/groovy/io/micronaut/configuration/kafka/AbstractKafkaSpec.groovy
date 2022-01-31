package io.micronaut.configuration.kafka

import spock.lang.Specification
import spock.util.concurrent.PollingConditions

abstract class AbstractKafkaSpec extends Specification {

    protected final PollingConditions conditions = new PollingConditions(timeout: conditionsTimeout, delay: 1)

    protected int getConditionsTimeout() {
        30
    }

    protected Map<String, Object> getConfiguration() {
        ['spec.name': getClass().simpleName]
    }
}
