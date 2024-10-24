/*
 * Copyright 2017-2024 original authors
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
package io.micronaut.configuration.kafka.streams.health.serde;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.type.Argument;
import io.micronaut.serde.Decoder;
import io.micronaut.serde.Encoder;
import io.micronaut.serde.Serde;
import jakarta.inject.Singleton;
import org.apache.kafka.streams.processor.TaskId;

import java.io.IOException;


/**
 * A custom {@link Serde} implementation for serializing and deserializing
 * {@link TaskId}.
 *
 * @see TaskId
 * @see Serde
 */
@Singleton
public final class TaskIdSerde implements Serde<TaskId> {

    /**
     * Deserializes a {@link TaskId} from its string representation.
     *
     * @param decoder The {@link Decoder} used to read the input.
     * @param context The deserialization context.
     * @param type    The {@link TaskId} type argument.
     * @return The deserialized {@link TaskId}.
     * @throws IOException If an I/O error occurs during deserialization.
     */
    @Override
    public @Nullable TaskId deserialize(@NonNull Decoder decoder, DecoderContext context, @NonNull Argument<? super TaskId> type) throws IOException {
        final String taskIdStr = decoder.decodeString();
        return TaskId.parse(taskIdStr);
    }

    /**
     * Serializes a {@link TaskId} into its string representation.
     *
     * @param encoder The {@link Encoder} used to write the serialized output.
     * @param context The serialization context.
     * @param type    The {@link TaskId} type argument.
     * @param value   The {@link TaskId} instance to be serialized.
     * @throws IOException If an I/O error occurs during serialization.
     */
    @Override
    public void serialize(@NonNull Encoder encoder, EncoderContext context, @NonNull Argument<? extends TaskId> type, @NonNull TaskId value) throws IOException {
        encoder.encodeString(value.toString());
    }

}
