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
package io.micronaut.configuration.kafka.processor;

import io.micronaut.core.annotation.Internal;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.MonoSink;

/**
 * Internal {@link MonoSink} adapter for {@link Callback}.
 *
 * @author Guillermo Calvo
 * @since 5.2
 * @param emitter The {@link MonoSink} to invoke.
 */
@Internal
record MonoCallback(MonoSink<RecordMetadata> emitter) implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            emitter.error(exception);
        } else {
            emitter.success(metadata);
        }
    }
}
