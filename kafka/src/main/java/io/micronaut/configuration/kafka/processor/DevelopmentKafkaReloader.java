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

import io.micronaut.configuration.kafka.ProducerRegistry;
import io.micronaut.configuration.kafka.TransactionalProducerRegistry;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.serde.SerdeRegistry;
import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.WatchableBeanContext;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Executable;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.DevelopmentMode;
import io.micronaut.context.reload.ClassChange;
import io.micronaut.context.reload.ClassChangeEvent;
import io.micronaut.context.reload.ReloadStrategy;
import io.micronaut.context.watch.BeanDefinitionChange;
import io.micronaut.context.watch.BeanDefinitionWatcher;
import io.micronaut.context.watch.ClassChangeWatcher;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.order.Ordered;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.BeanDefinitionReference;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Keeps the Kafka consumers, producers and serdes in step with the code in development mode. It exists only in
 * development mode, so nothing of it is on the path of a record consumed or produced.
 *
 * <ul>
 *     <li>A {@link KafkaListener} bean definition registered or removed, or a listener class changed in place,
 *     restarts the consumers: the {@link KafkaConsumerProcessor} is recreated, which stops and closes every consumer
 *     it started, the changed listener beans are recreated, and the new processor is given the listener methods, as
 *     at startup, so that it starts their consumers again.</li>
 *     <li>A class change applied in place that retires a classloader, or that changes a serializer, a deserializer,
 *     a serde, a serde registry or a {@link KafkaClient}, recreates the serde registries and the producer registries,
 *     whose maps are keyed by the classes of the generation that filled them. The beans that received them, the
 *     {@link KafkaClient} advice with its producers and the consumer processor among them, are destroyed with them,
 *     as the dependency graph records; the consumers are then started again on top of the new beans.</li>
 *     <li>A class change applied in place that touches none of these starts the consumers again when the processor
 *     is gone: another module that recreated a bean it received, such as a JSON mapper, destroyed it with its
 *     consumers.</li>
 * </ul>
 *
 * <p>A change that restarts the application is ignored: the new context starts new consumers, and the one it
 * replaces closes its own as it stops. The processor has no way to stop the consumers of one bean, so a change of
 * one listener restarts all of them. Each bean is recreated through {@link WatchableBeanContext#recreate(Object)};
 * a context that does not track bean dependencies recreates nothing, and the consumers keep running as they are
 * until a restart.</p>
 *
 * <p>The configuration is not watched: every Kafka configuration bean requires the {@code KafkaDefaultConfiguration}
 * bean, which binds all of {@code kafka}, so the development runtime restarts the application for any change under
 * it, and the new context builds its clients from the new values.</p>
 *
 * <p>The watches run after those of other modules, so that a serde or a mapper another module recreates for the
 * same change, which destroys the processor that received it, is in place before the consumers start again. It
 * holds the context only, never a Kafka bean: a bean that received one is a dependent of it, which recreating it
 * would destroy along with its watches.</p>
 *
 * @author graemerocher
 * @since 6.3.0
 */
@Internal
@Context
@Requires(condition = DevelopmentMode.Active.class)
final class DevelopmentKafkaReloader {

    private static final Logger LOG = LoggerFactory.getLogger(DevelopmentKafkaReloader.class);

    /**
     * The types whose beans cache by class: the serde registries, and the producer registries with the producers
     * they built for a key and value type.
     */
    private static final List<Class<?>> TYPE_CACHES = List.of(SerdeRegistry.class, ProducerRegistry.class, TransactionalProducerRegistry.class);

    /**
     * The types of a class whose change makes the serdes and producers built from it stale.
     */
    private static final List<Class<?>> SERDE_TYPES = List.of(Serializer.class, Deserializer.class, Serde.class, SerdeRegistry.class);

    private final BeanContext beanContext;

    /**
     * @param beanContext The context, watched when it can be
     */
    DevelopmentKafkaReloader(BeanContext beanContext) {
        this.beanContext = beanContext;
        if (beanContext instanceof WatchableBeanContext watchable) {
            watchable.watchDefinitions(Object.class, Qualifiers.byStereotype(KafkaListener.class), new ListenerDefinitionsWatcher());
            watchable.watchClassChanges(new ClassWatcher());
        }
    }

    private void onClassChange(ClassChangeEvent change) {
        if (change.strategy() == ReloadStrategy.RESTART) {
            // the new context starts its own consumers, and the one it replaces closes these as it stops
            return;
        }
        if (!change.retiredLoaders().isEmpty()) {
            restartConsumers(true, List.of(), "a reload retired a classloader");
            return;
        }
        boolean typeCaches = false;
        List<String> listeners = new ArrayList<>();
        for (ClassChange classChange : change.changes()) {
            String className = classChange.className();
            Class<?> type = load(className, change.newLoader());
            if (isListener(type) || wasCompiledWith(className, KafkaListener.class)) {
                listeners.add(className);
            }
            if (isSerdeOrClient(type) || wasCompiledWith(className, KafkaClient.class) || wasSerde(className)) {
                typeCaches = true;
            }
        }
        if (typeCaches || !listeners.isEmpty()) {
            restartConsumers(typeCaches, listeners, typeCaches ? "a serde or a Kafka client changed" : listeners + " changed");
        } else {
            // a module that recreated a bean the processor received for this change destroyed the processor too
            startConsumersIfStopped();
        }
    }

    private static Class<?> load(String className, ClassLoader loader) {
        try {
            return Class.forName(className, false, loader);
        } catch (ClassNotFoundException | LinkageError e) {
            // removed, or not loadable on its own: nothing of the new generation is built from it
            return null;
        }
    }

    private static boolean isListener(Class<?> type) {
        return type != null && hasAnnotation(type.getAnnotations(), KafkaListener.class);
    }

    private static boolean isSerdeOrClient(Class<?> type) {
        if (type == null) {
            return false;
        }
        for (Class<?> serdeType : SERDE_TYPES) {
            if (serdeType.isAssignableFrom(type)) {
                return true;
            }
        }
        return hasAnnotation(type.getAnnotations(), KafkaClient.class);
    }

    /**
     * Whether an annotation is present, or is the stereotype of one that is.
     */
    private static boolean hasAnnotation(Annotation[] annotations, Class<? extends Annotation> wanted) {
        for (Annotation annotation : annotations) {
            Class<? extends Annotation> annotationType = annotation.annotationType();
            if (annotationType == wanted || annotationType.isAnnotationPresent(wanted)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the class a change replaces carried a stereotype as the context was compiled: a bean definition of it,
     * or of its proxy, has it on the class or an executable method. The references are matched by name, so that only
     * the definitions of that class are loaded, and nothing of a previous class is kept.
     *
     * @param className The changed class
     * @param stereotype The stereotype
     * @return Whether it was there
     */
    private boolean wasCompiledWith(String className, Class<? extends Annotation> stereotype) {
        for (BeanDefinitionReference<?> reference : definitionsOf(className)) {
            try {
                if (reference.getAnnotationMetadata().hasStereotype(stereotype)) {
                    return true;
                }
                BeanDefinition<?> definition = reference.load();
                if (definition == null) {
                    continue;
                }
                for (ExecutableMethod<?, ?> method : definition.getExecutableMethods()) {
                    if (method.getAnnotationMetadata().hasStereotype(stereotype)) {
                        return true;
                    }
                }
            } catch (RuntimeException | LinkageError e) {
                // a definition of that name that no longer loads: what it was is unknown, so it counts
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the class a change replaces was a serde or a serde registry bean as the context was compiled.
     */
    private boolean wasSerde(String className) {
        for (BeanDefinitionReference<?> reference : definitionsOf(className)) {
            try {
                Class<?> beanType = reference.getBeanType();
                for (Class<?> serdeType : SERDE_TYPES) {
                    if (serdeType.isAssignableFrom(beanType)) {
                        return true;
                    }
                }
            } catch (RuntimeException | LinkageError e) {
                return true;
            }
        }
        return false;
    }

    private List<BeanDefinitionReference<?>> definitionsOf(String className) {
        int lastDot = className.lastIndexOf('.');
        // $Name$Definition, the definitions of its proxies and, for an introduced interface, $Name$Intercepted$Definition
        String prefix = className.substring(0, lastDot + 1) + '$' + className.substring(lastDot + 1);
        String definition = prefix + "$Definition";
        List<BeanDefinitionReference<?>> references = new ArrayList<>();
        for (BeanDefinitionReference<?> reference : beanContext.getBeanDefinitionReferences()) {
            String name = reference.getBeanDefinitionName();
            if (name.equals(definition) || name.startsWith(definition + '$') || name.startsWith(prefix + "$Intercepted$Definition")) {
                references.add(reference);
            }
        }
        return references;
    }

    /**
     * Stops every consumer, recreates what changed, and starts the consumers again on top of the new beans. Nothing
     * is created that was not created already, but for the processor and the listener beans it starts.
     *
     * @param typeCaches Whether to recreate the serde and producer registries, and the beans that received them
     * @param listeners The listener classes that changed, whose beans are recreated
     * @param reason Why, for the log
     */
    private void restartConsumers(boolean typeCaches, List<String> listeners, String reason) {
        if (!(beanContext instanceof WatchableBeanContext context)) {
            return;
        }
        // taken first: recreating one destroys the beans that received it, as the graph records them. The processor
        // goes first, so that its consumers stop before anything they call is destroyed
        List<Object> beans = new ArrayList<>();
        for (BeanRegistration<KafkaConsumerProcessor> registration : beanContext.getActiveBeanRegistrations(KafkaConsumerProcessor.class)) {
            add(beans, registration.bean());
        }
        if (typeCaches) {
            for (Class<?> type : TYPE_CACHES) {
                for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(type)) {
                    add(beans, registration.bean());
                }
            }
        }
        if (!listeners.isEmpty()) {
            Set<String> names = new HashSet<>(listeners);
            for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(Qualifiers.byStereotype(KafkaListener.class))) {
                if (names.contains(registration.getBeanDefinition().getBeanType().getName())) {
                    add(beans, registration.bean());
                }
            }
        }
        if (beans.isEmpty()) {
            startConsumersIfStopped();
            return;
        }
        LOG.debug("Restarting the Kafka consumers: {}", reason);
        boolean recreated = false;
        for (Object bean : beans) {
            // false for a bean destroyed with one recreated before it, and for all of them in a context that does
            // not track bean dependencies: they are kept, and the consumers keep running until a restart
            recreated |= context.recreate(bean);
        }
        if (recreated) {
            startConsumers();
        }
    }

    /**
     * Starts the consumers when the processor is gone: a bean it received was recreated, by this reloader or by
     * another module, and destroyed it, and with it every consumer it had started.
     */
    private void startConsumersIfStopped() {
        if (beanContext.getActiveBeanRegistrations(KafkaConsumerProcessor.class).isEmpty() && !listenerMethods().isEmpty()) {
            LOG.debug("Starting the Kafka consumers again: the consumer processor was destroyed");
            startConsumers();
        }
    }

    /**
     * Gives the current processor, created now if it is gone, every listener method, as the context does at startup.
     */
    private void startConsumers() {
        List<ListenerMethod> methods = listenerMethods();
        if (methods.isEmpty()) {
            return;
        }
        KafkaConsumerProcessor processor = beanContext.findBean(KafkaConsumerProcessor.class).orElse(null);
        if (processor == null) {
            return;
        }
        for (ListenerMethod entry : methods) {
            try {
                processor.process(entry.definition(), entry.method());
            } catch (RuntimeException e) {
                LOG.warn("The Kafka listener {} could not be started again: {}", entry.method(), e.getMessage(), e);
            }
        }
    }

    /**
     * The methods the context gives the processor at startup: those a listener marks for processing that carry
     * {@link Topic}.
     */
    @SuppressWarnings("unchecked")
    private List<ListenerMethod> listenerMethods() {
        List<ListenerMethod> methods = new ArrayList<>();
        for (BeanDefinition<?> definition : beanContext.getBeanDefinitions(Qualifiers.byStereotype(KafkaListener.class))) {
            if (!definition.requiresMethodProcessing()) {
                continue;
            }
            for (ExecutableMethod<?, ?> method : definition.getExecutableMethodsForProcessing()) {
                AnnotationMetadata metadata = method.getAnnotationMetadata();
                if (metadata.getAnnotationTypesByStereotype(Executable.class).contains(Topic.class)) {
                    methods.add(new ListenerMethod((BeanDefinition<Object>) definition, (ExecutableMethod<Object, ?>) method));
                }
            }
        }
        return methods;
    }

    private static void add(List<Object> beans, Object bean) {
        for (Object taken : beans) {
            if (taken == bean) {
                return;
            }
        }
        beans.add(bean);
    }

    /**
     * Restarts the consumers when a listener definition is registered or removed. The first batch is what the
     * processor was given at startup.
     */
    private final class ListenerDefinitionsWatcher implements BeanDefinitionWatcher<Object>, Ordered {
        @Override
        public void onChange(BeanDefinitionChange<Object> change) {
            if (change.initial() || (change.added().isEmpty() && change.removed().isEmpty())) {
                return;
            }
            List<String> changed = new ArrayList<>();
            for (BeanDefinition<Object> definition : change.removed()) {
                changed.add(definition.getBeanType().getName());
            }
            for (BeanDefinition<Object> definition : change.added()) {
                changed.add(definition.getBeanType().getName());
            }
            restartConsumers(false, changed, "listener definitions changed");
        }

        @Override
        public int getOrder() {
            return Ordered.LOWEST_PRECEDENCE;
        }
    }

    /**
     * Follows a class change applied in place.
     */
    private final class ClassWatcher implements ClassChangeWatcher, Ordered {
        @Override
        public void onChange(ClassChangeEvent change) {
            onClassChange(change);
        }

        @Override
        public int getOrder() {
            return Ordered.LOWEST_PRECEDENCE;
        }
    }

    /**
     * A listener method, and the definition that declares it.
     *
     * @param definition The definition
     * @param method The method
     */
    private record ListenerMethod(BeanDefinition<Object> definition, ExecutableMethod<Object, ?> method) {
    }
}
