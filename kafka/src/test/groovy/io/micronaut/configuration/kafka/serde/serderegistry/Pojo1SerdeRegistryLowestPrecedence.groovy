package io.micronaut.configuration.kafka.serde.serderegistry

import groovy.transform.CompileStatic
import io.micronaut.configuration.kafka.serde.SerdeRegistry
import io.micronaut.core.order.Ordered
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

@CompileStatic
class Pojo1SerdeRegistryLowestPrecedence implements SerdeRegistry, Ordered {

    int order = LOWEST_PRECEDENCE

    static class StringSerde extends Serdes.WrapperSerde<String> {
        StringSerde() {
            super(new StringSerializer(), new StringDeserializer())
        }
    }

    @Override
    <T> Serde<T> getSerde(Class<T> type) {
        if (type == Pojo1) {
            return new StringSerde() as Serde<T>
        }
        return null
    }
}
