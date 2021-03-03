package io.micronaut.configuration.kafka

import spock.lang.Specification
import spock.util.concurrent.PollingConditions

abstract class AbstractKafkaSpec extends Specification {

    protected final PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

    protected Map<String, Object> getConfiguration() {
        Map<String, Object> config = [:]
        config['spec.name'] = getClass().getSimpleName()
        config
    }
}
