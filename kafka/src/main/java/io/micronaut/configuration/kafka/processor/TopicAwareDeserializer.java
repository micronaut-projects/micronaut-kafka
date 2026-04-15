/*
 * Copyright 2017-2026 original authors
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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Delegates deserialization based on the topic being consumed.
 *
 * @author Micronaut Framework Team
 * @since 6.0.0
 */
@Internal
final class TopicAwareDeserializer implements Deserializer<Object> {

    private final TopicRouter<? extends Deserializer<?>> router;
    private final String kind;

    TopicAwareDeserializer(TopicRouter<? extends Deserializer<?>> router, String kind) {
        this.router = router;
        this.kind = kind;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        for (Deserializer<?> deserializer : uniqueDelegates()) {
            deserializer.configure(configs, isKey);
        }
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return router.resolve(topic, kind + " deserializer").deserialize(topic, data);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return router.resolve(topic, kind + " deserializer").deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        for (Deserializer<?> deserializer : uniqueDelegates()) {
            deserializer.close();
        }
    }

    private Set<Deserializer<?>> uniqueDelegates() {
        Set<Deserializer<?>> delegates = Collections.newSetFromMap(new IdentityHashMap<>());
        delegates.addAll(router.values());
        return delegates;
    }
}
