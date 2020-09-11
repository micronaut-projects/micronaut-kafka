package io.micronaut.configuration.kafka.serde.serderegistry

import groovy.transform.CompileStatic
import io.micronaut.configuration.kafka.serde.SerdeRegistry
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

@CompileStatic
class Pojo1SerdeRegistryDefaultPrecedence implements SerdeRegistry {

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
