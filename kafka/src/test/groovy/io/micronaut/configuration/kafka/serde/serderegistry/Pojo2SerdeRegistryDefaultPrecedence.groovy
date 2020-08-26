package io.micronaut.configuration.kafka.serde.serderegistry

import groovy.transform.CompileStatic
import io.micronaut.configuration.kafka.serde.SerdeRegistry
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes

@CompileStatic
class Pojo2SerdeRegistryDefaultPrecedence implements SerdeRegistry {

    static class IntegerSerde extends Serdes.WrapperSerde<Integer> {
        IntegerSerde() {
            super(new IntegerSerializer(), new IntegerDeserializer())
        }
    }
    @Override
    <T> Serde<T> getSerde(Class<T> type) {
        if (type == Pojo2) {
            return new IntegerSerde() as Serde<T>
        }
        return null
    }
}
