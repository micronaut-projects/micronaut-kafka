package io.micronaut.configuration.kafka.scope;

import io.micronaut.context.scope.AbstractConcurrentCustomScope;
import io.micronaut.context.scope.CreatedBean;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.inject.BeanIdentifier;
import jakarta.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

/**
 * This scope must be bound to a thread, likely the one that the current consumer
 * poll / something or other is executing on
 *
 * Map of consumers + threads ?
 *
 * using ThreadLocal will never refresh the bean as the thread keeps running, so this needs to be _similar_
 * but also have a hook into the polling of the consumer :thinking:
 *
 */
@Singleton
public class PollCustomScope extends AbstractConcurrentCustomScope<PollScope> {

    private final ThreadLocal<Map<BeanIdentifier, CreatedBean<?>>> threadScope = ThreadLocal.withInitial(HashMap::new);

    public PollCustomScope() { super(PollScope.class); }

    @Override
    protected @NonNull Map<BeanIdentifier, CreatedBean<?>> getScopeMap(boolean forCreation) {
        /**
         * get what consumer/thread is executing, and tie the bean to that thread?
         * Can/should I use ThreadLocal at all?
         */
        return null;

    }

    @Override
    public boolean isRunning() {
        /**
         * TODO is there some context / tool that I can hook into that tells me
         *  what kafka thing is executing?
         *
         * If any of the consumers are currently not paused / are polling?
         */
        return false;
    }

    @Override
    public void close() {

    }
}
