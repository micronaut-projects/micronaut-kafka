package io.micronaut.configuration.kafka.scope;

import io.micronaut.configuration.kafka.event.KafkaConsumerProcessingEvent;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.scope.AbstractConcurrentCustomScope;
import io.micronaut.context.scope.CreatedBean;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.inject.BeanIdentifier;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.Consumer;

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
 * should this implement application event listener to accomplish that? We still need some way to get an identifier
 * or something that tells us what thread is associated with what consumer, ThreadLocal will just get us the current thread
 *
 * how would this work with services/classes that might be shared between a consumer and a http service?
 *  update it at the first sign of calling for it? What happens when it is annotated with PollScope and RequestScope?
 *
 * What happens in kotlin when you eventually specify something like withContext(Dispatchers.IO)? does a new thread get started?
 *
 * could this also maybe be done using RefreshScope? call to refresh the bean every time you poll in your own consumer maybe? just trigger the refresh event
 * by publishing an application event? it feels still like the tricky part is "only update this bean for this consumer's execution"
 *
 * - could a better (and easier) solution be accomplished in some other way? bean provider?
 * - some AOP advice - "every time this function runs, update all beans of this type"?
 *    - feels kinda similar to refreshable, but driven by annotations
 */
@Singleton
public class PollCustomScope extends AbstractConcurrentCustomScope<PollScope> implements ApplicationEventListener<KafkaConsumerProcessingEvent> {

    private final ThreadLocal<Map<BeanIdentifier, CreatedBean<?>>> threadScope = ThreadLocal.withInitial(HashMap::new);

    public PollCustomScope() { super(PollScope.class); }

    /**
     * called every time the bean is requested/needed.
     * @param forCreation Whether it is for creation
     * @return
     */
    @Override
    protected @NonNull Map<BeanIdentifier, CreatedBean<?>> getScopeMap(boolean forCreation) {
        /**
         * get what consumer/thread is executing, and tie the bean to that thread?
         */
        return threadScope.get();
    }

    @Override
    public boolean isRunning() {
        /**
         * TODO is there some context / tool that I can hook into that tells me
         *  what kafka thing is executing?
         *  If any of the consumers are currently not paused / are polling?
         */
        return true;
    }

    @Override
    public void close() {
        // ????
        threadScope.remove();
    }

    @Override
    public void onApplicationEvent(KafkaConsumerProcessingEvent event) {
        // TODO update / refresh the bean associated with this consumer on this thread?
        event.getSource();
    }
}
