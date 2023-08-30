/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.serde;

import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.core.annotation.Creator;
import io.micronaut.core.serialize.exceptions.SerializationException;
import io.micronaut.json.JsonObjectSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * A {@link Serializer} and {@link Deserializer} for JSON.
 *
 * @param <T> The type to serialize/deserialize
 * @author Denis Stepanov
 * @since 4.1.0
 */
@Prototype
public class JsonObjectSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {

    private final JsonObjectSerializer objectSerializer;
    private final Class<T> type;

    /**
     * Constructs a new instance for the given arguments.
     *
     * @param objectSerializer The {@link JsonObjectSerializer}
     * @param type The target type
     */
    @Creator
    public JsonObjectSerde(JsonObjectSerializer objectSerializer, @Parameter Class<T> type) {
        this.objectSerializer = objectSerializer;
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return objectSerializer.deserialize(data, type)
                .orElseThrow(() -> new SerializationException("Unable to deserialize data: " + data));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        return objectSerializer.serialize(data)
                .orElseThrow(() -> new SerializationException("Unable to serialize data: " + data));
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public String toString() {
        return "JsonObjectSerde: " + type.getName();
    }
}
