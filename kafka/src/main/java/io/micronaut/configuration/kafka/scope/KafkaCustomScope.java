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
package io.micronaut.configuration.kafka.scope;

import io.micronaut.configuration.kafka.annotation.KafkaScope;
import io.micronaut.context.scope.AbstractConcurrentCustomScope;
import io.micronaut.context.scope.CreatedBean;
import io.micronaut.core.annotation.Internal;
import io.micronaut.inject.BeanIdentifier;
import jakarta.inject.Singleton;
import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Stores {@link KafkaScope} beans for the duration of a Kafka listener invocation.
 *
 * @author graemerocher
 * @since 5.5.0
 */
@Singleton
@Internal
public final class KafkaCustomScope extends AbstractConcurrentCustomScope<KafkaScope> {

    private final java.lang.ThreadLocal<Deque<ScopeEntry>> scopes = new java.lang.ThreadLocal<>();

    public KafkaCustomScope() {
        super(KafkaScope.class);
    }

    /**
     * Activates the Kafka scope on the current thread.
     *
     * @return A handle that closes the current scope when the invocation ends
     */
    public Scope open() {
        Deque<ScopeEntry> stack = scopes.get();
        if (stack == null) {
            stack = new ArrayDeque<>(1);
            scopes.set(stack);
        }
        ScopeEntry entry = new ScopeEntry();
        stack.addLast(entry);
        return () -> close(entry);
    }

    /**
     * Executes an action within a Kafka scope.
     *
     * @param action The action to execute
     * @param <T> The result type
     * @return The action result
     */
    public <T> T execute(Supplier<T> action) {
        try (Scope scope = open()) {
            return action.get();
        }
    }

    /**
     * Executes an action within a Kafka scope.
     *
     * @param action The action to execute
     */
    public void execute(Runnable action) {
        execute(() -> {
            action.run();
            return null;
        });
    }

    @Override
    public boolean isRunning() {
        return true;
    }

    @Override
    public void close() {
        scopes.remove();
    }

    @Override
    protected @Nullable Map<BeanIdentifier, CreatedBean<?>> getScopeMap(boolean forCreation) {
        Deque<ScopeEntry> stack = scopes.get();
        if (stack == null || stack.isEmpty()) {
            if (forCreation) {
                throw new IllegalStateException("No active Kafka scope");
            }
            return null;
        }
        return stack.peekLast().beans;
    }

    private void close(ScopeEntry expected) {
        Deque<ScopeEntry> stack = scopes.get();
        if (stack == null || stack.isEmpty()) {
            return;
        }
        ScopeEntry current = stack.removeLast();
        if (current != expected) {
            throw new IllegalStateException("Kafka scope closed out of order");
        }
        destroyScope(current.beans);
        if (stack.isEmpty()) {
            scopes.remove();
        }
    }

    /**
     * Handle for an active Kafka scope.
     */
    @FunctionalInterface
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    private static final class ScopeEntry {
        private final Map<BeanIdentifier, CreatedBean<?>> beans = new HashMap<>(2);
    }
}
