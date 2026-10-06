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
package io.micronaut.configuration.kafka.streams;

import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.WatchableBeanContext;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.DevelopmentMode;
import io.micronaut.context.reload.ClassChange;
import io.micronaut.context.reload.ClassChangeEvent;
import io.micronaut.context.reload.ReloadStrategy;
import io.micronaut.context.watch.ClassChangeWatcher;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.order.Ordered;
import io.micronaut.inject.BeanDefinition;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rebuilds the Kafka Streams in development mode when the code their topology was built from changed. It exists only
 * in development mode, so nothing of it is on the path of a stream.
 *
 * <ul>
 *     <li>A class change applied in place that retires a classloader, or that changes a class declaring a
 *     {@link KStream}, {@link KTable} or {@link GlobalKTable} bean, such as the factory that builds a topology,
 *     rebuilds the streams.</li>
 * </ul>
 *
 * <p>A {@link KafkaStreams} is built once, from a topology its builder cannot build again. A rebuild closes the
 * streams, leaving their consumer groups, then recreates through {@link WatchableBeanContext#recreate(Object)} the
 * {@link KafkaStreamsFactory}, the stream builders, the topology beans and the streams, in that order, and asks for
 * the streams again, which the context otherwise creates only at startup. The factory closes its streams without
 * leaving their groups: the streams built again with the same application id would otherwise wait for the group to
 * time the old members out, 45 seconds by default, before they get any partition.</p>
 *
 * <p>A change that restarts the application rebuilds nothing, since the new context builds its own streams; the
 * streams are closed here, leaving their groups, before the context that stops closes them again. A context that
 * does not track bean dependencies recreates nothing, and the streams keep running as they are until a restart.</p>
 *
 * <p>The configuration is not watched: the streams configuration beans require the Kafka configuration bean, which
 * binds all of {@code kafka}, so the development runtime restarts the application for any change under it.</p>
 *
 * <p>The watches run after those of other modules. It holds the context only, never a Kafka Streams bean: a bean
 * that received one is a dependent of it, which recreating it would destroy along with its watches.</p>
 *
 * @author graemerocher
 * @since 6.3.0
 */
@Internal
@Context
@Requires(condition = DevelopmentMode.Active.class)
final class DevelopmentKafkaStreamsReloader {

    private static final Logger LOG = LoggerFactory.getLogger(DevelopmentKafkaStreamsReloader.class);

    /**
     * The beans a topology is built from.
     */
    private static final List<Class<?>> TOPOLOGY_TYPES = List.of(KStream.class, KTable.class, GlobalKTable.class);

    /**
     * The beans a rebuild replaces after the factory, in order: the builders, the topology beans and the streams.
     */
    private static final List<Class<?>> REBUILT_TYPES = List.of(ConfiguredStreamBuilder.class, KStream.class, KTable.class, GlobalKTable.class, KafkaStreams.class);

    /**
     * How long a rebuild waits for a stream to close.
     */
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);

    private final BeanContext beanContext;

    /**
     * @param beanContext The context, watched when it can be
     */
    DevelopmentKafkaStreamsReloader(BeanContext beanContext) {
        this.beanContext = beanContext;
        if (beanContext instanceof WatchableBeanContext watchable) {
            watchable.watchClassChanges(new ClassWatcher());
        }
    }

    /**
     * Follows a class change applied in place, after the watches of other modules.
     */
    private final class ClassWatcher implements ClassChangeWatcher, Ordered {
        @Override
        public void onChange(ClassChangeEvent change) {
            if (change.strategy() == ReloadStrategy.RESTART) {
                // the new context builds its own streams, and the one it replaces closes these as it stops; they leave
                // their groups first, so that the new streams need not wait for the group to time them out
                leaveGroups();
                return;
            }
            if (!change.retiredLoaders().isEmpty()) {
                rebuild("a reload retired a classloader");
                return;
            }
            Set<String> topologyClasses = topologyClasses();
            for (ClassChange classChange : change.changes()) {
                if (topologyClasses.contains(classChange.className())) {
                    rebuild(classChange.className() + " changed");
                    return;
                }
            }
        }

        @Override
        public int getOrder() {
            return Ordered.LOWEST_PRECEDENCE;
        }
    }

    /**
     * The classes a topology is built from, as the context was compiled: the declaring factory of a topology bean,
     * or its type. A change applied in place cannot change the definitions, so they name the classes the running
     * topology was built from.
     */
    private Set<String> topologyClasses() {
        Set<String> classes = new HashSet<>();
        for (Class<?> type : TOPOLOGY_TYPES) {
            for (BeanDefinition<?> definition : beanContext.getBeanDefinitions(type)) {
                classes.add(definition.getBeanType().getName());
                definition.getDeclaringType().ifPresent(declaring -> classes.add(declaring.getName()));
            }
        }
        return classes;
    }

    /**
     * Closes the streams and builds them again on top of new builders and topology beans. Nothing is built when the
     * context holds no streams factory.
     *
     * @param reason Why, for the log
     */
    private void rebuild(String reason) {
        if (!(beanContext instanceof WatchableBeanContext context)
            || beanContext.getActiveBeanRegistrations(KafkaStreamsFactory.class).isEmpty()) {
            return;
        }
        // taken first: recreating one destroys the beans that received it, as the graph records them. The factory goes
        // first, so that the streams it started are closed before anything they run is destroyed
        List<Object> beans = new ArrayList<>();
        for (BeanRegistration<KafkaStreamsFactory> registration : beanContext.getActiveBeanRegistrations(KafkaStreamsFactory.class)) {
            add(beans, registration.bean());
        }
        // then what the streams were built from, and the streams: a bean the graph did not destroy with the factory is
        // recreated on its own, so that no stream is built again on a builder that built its topology already
        for (Class<?> type : REBUILT_TYPES) {
            for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(type)) {
                add(beans, registration.bean());
            }
        }
        LOG.debug("Rebuilding the Kafka Streams: {}", reason);
        if (context.findDependencyGraph().isPresent()) {
            leaveGroups();
        }
        boolean recreated = false;
        for (Object bean : beans) {
            // false for a bean destroyed with one recreated before it, and for all of them in a context that does
            // not track bean dependencies: they are kept, and the streams keep running until a restart
            recreated |= context.recreate(bean);
        }
        if (!recreated) {
            return;
        }
        try {
            // the streams are context beans: nothing else asks for them again
            beanContext.getBeansOfType(KafkaStreams.class);
        } catch (RuntimeException e) {
            LOG.warn("The Kafka Streams could not be built again: {}", e.getMessage(), e);
        }
    }

    /**
     * Closes the streams, leaving their consumer groups: the factory closes a stream without leaving, so the streams
     * built again with the same application id would wait for the group to time the old members out before they
     * get any partition. A stream closed here is closed already when the factory closes it.
     */
    private void leaveGroups() {
        // every stream the factories started, those the context does not hold among them
        for (BeanRegistration<KafkaStreamsFactory> registration : beanContext.getActiveBeanRegistrations(KafkaStreamsFactory.class)) {
            for (Map.Entry<KafkaStreams, ConfiguredStreamBuilder> stream : registration.bean().getStreams().entrySet()) {
                try {
                    stream.getKey().close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP).withTimeout(CLOSE_TIMEOUT));
                } catch (RuntimeException e) {
                    LOG.debug("The Kafka Streams {} could not leave their group: {}", stream.getValue().getName(), e.getMessage(), e);
                }
            }
        }
    }

    private static void add(List<Object> beans, Object bean) {
        for (Object taken : beans) {
            if (taken == bean) {
                return;
            }
        }
        beans.add(bean);
    }
}
