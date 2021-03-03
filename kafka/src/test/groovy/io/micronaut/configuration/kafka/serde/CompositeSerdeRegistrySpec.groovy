package io.micronaut.configuration.kafka.serde

import io.micronaut.configuration.kafka.serde.serderegistry.Pojo1
import io.micronaut.configuration.kafka.serde.serderegistry.Pojo1SerdeRegistryDefaultPrecedence
import io.micronaut.configuration.kafka.serde.serderegistry.Pojo1SerdeRegistryHighPrededence
import io.micronaut.configuration.kafka.serde.serderegistry.Pojo1SerdeRegistryLowestPrecedence
import io.micronaut.configuration.kafka.serde.serderegistry.Pojo2SerdeRegistryDefaultPrecedence
import org.apache.kafka.common.serialization.Serde
import spock.lang.Specification

class CompositeSerdeRegistrySpec extends Specification {

    void "test highest precedence serde"() {
        setup:
        CompositeSerdeRegistry registry = new CompositeSerdeRegistry(
                new Pojo2SerdeRegistryDefaultPrecedence(),
                new Pojo1SerdeRegistryDefaultPrecedence(),
                new Pojo1SerdeRegistryLowestPrecedence(),
                new Pojo1SerdeRegistryHighPrededence()
        )

        when:
        Serde<?> serde = registry.getSerde(Pojo1)

        then:
        serde.class == Pojo1SerdeRegistryHighPrededence.StringSerde
    }

    void "test default precedence serde"() {
        setup:
        CompositeSerdeRegistry registry = new CompositeSerdeRegistry(
                new Pojo2SerdeRegistryDefaultPrecedence(),
                new Pojo1SerdeRegistryDefaultPrecedence(),
                new Pojo1SerdeRegistryLowestPrecedence()
        )

        when:
        Serde<?> serde = registry.getSerde(Pojo1)

        then:
        serde.class == Pojo1SerdeRegistryDefaultPrecedence.StringSerde
    }

    void "test lowest precedence serde"() {
        setup:
        CompositeSerdeRegistry registry = new CompositeSerdeRegistry(
                new Pojo2SerdeRegistryDefaultPrecedence(),
                new Pojo1SerdeRegistryLowestPrecedence()
        )

        when:
        Serde<?> serde = registry.getSerde(Pojo1)

        then:
        serde.class == Pojo1SerdeRegistryLowestPrecedence.StringSerde
    }
}
