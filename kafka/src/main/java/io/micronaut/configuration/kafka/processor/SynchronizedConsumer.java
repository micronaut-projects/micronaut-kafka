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
package io.micronaut.configuration.kafka.processor;

import io.micronaut.core.annotation.Internal;
import org.apache.kafka.clients.consumer.Consumer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

/**
 * Creates a synchronized {@link Consumer} view that shares a monitor with Micronaut's poll loop.
 * The {@code wakeup} method remains unsynchronized because Kafka documents it as the only thread-safe method.
 *
 * @author graemerocher
 * @since 5.7.0
 */
@Internal
final class SynchronizedConsumer {

    private SynchronizedConsumer() {
    }

    @SuppressWarnings("unchecked")
    static <K, V> Consumer<K, V> wrap(Consumer<K, V> consumer, Object monitor) {
        return (Consumer<K, V>) Proxy.newProxyInstance(
            consumer.getClass().getClassLoader(),
            new Class<?>[] { Consumer.class },
            (proxy, method, args) -> {
                if (method.getDeclaringClass() == Object.class || method.getName().equals("wakeup")) {
                    return invoke(consumer, method, args);
                }
                synchronized (monitor) {
                    return invoke(consumer, method, args);
                }
            }
        );
    }

    private static Object invoke(Consumer<?, ?> consumer, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(consumer, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
