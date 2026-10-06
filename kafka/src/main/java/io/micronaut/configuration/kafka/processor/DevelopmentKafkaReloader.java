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
import io.micronaut.configuration.kafka.serde.SerdeRegistry;
import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.WatchableBeanContext;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.DevelopmentMode;
import io.micronaut.context.reload.ClassChange;
import io.micronaut.context.reload.ClassChangeEvent;
import io.micronaut.context.reload.ReloadStrategy;
import io.micronaut.context.watch.BeanDefinitionChange;
import io.micronaut.context.watch.BeanDefinitionWatcher;
import io.micronaut.context.watch.ClassChangeWatcher;
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
 *     restarts the consumers: the changed listener beans are recreated, then the {@link KafkaConsumerProcessor},
 *     which stops and closes every consumer it started.</li>
 *     <li>A serializer, deserializer, serde or serde registry bean definition registered or removed recreates the
 *     serde registries and the producer registries, as a class change of one does.</li>
 *     <li>A class change applied in place that retires a classloader, or that changes a serializer, a deserializer,
 *     a serde, a serde registry, a factory that produces one of these, or a {@link KafkaClient}, recreates the serde registries and the producer registries,
 *     whose maps are keyed by the classes of the generation that filled them. The beans that received them, the
 *     {@link KafkaClient} advice with its producers and the consumer processor among them, are destroyed with them,
 *     as the dependency graph records.</li>
 *     <li>A class change applied in place that touches none of these recreates nothing.</li>
 * </ul>
 *
 * <p>The context creates a recreated processor, or one destroyed as a dependent of a recreated bean, again at once,
 * and gives it the listener methods it gave it at startup, so that it starts the consumers again on top of the new
 * beans. That covers a bean the processor received that another module recreates, such as a JSON mapper.</p>
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
 * same change is in place before this reloader restarts the consumers. It holds the context only, never a Kafka bean: a bean that received one is a dependent of it, which recreating it
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
    @SuppressWarnings("unchecked")
    DevelopmentKafkaReloader(BeanContext beanContext) {
        this.beanContext = beanContext;
        if (beanContext instanceof WatchableBeanContext watchable) {
            watchable.watchDefinitions(Object.class, Qualifiers.byStereotype(KafkaListener.class), new ListenerDefinitionsWatcher());
            for (Class<?> type : SERDE_TYPES) {
                watchable.watchDefinitions((Class<Object>) type, null, new SerdeDefinitionsWatcher());
            }
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
        // a change that touches none of these needs nothing: a module that recreated a bean the processor received,
        // such as a JSON mapper, destroyed the processor with it, and the context created it again and restarted it
        if (typeCaches || !listeners.isEmpty()) {
            restartConsumers(typeCaches, listeners, typeCaches ? "a serde or a Kafka client changed" : listeners + " changed");
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
     * Whether the class a change replaces was a serde or a serde registry bean as the context was compiled, or a
     * factory that produced one: a product of a factory is defined by {@code $Factory$MethodN$Definition}, whose bean
     * type is the product and whose declaring type is the factory.
     */
    private boolean wasSerde(String className) {
        for (BeanDefinitionReference<?> reference : definitionsOf(className)) {
            try {
                if (isSerdeType(reference.getBeanType())) {
                    return true;
                }
            } catch (RuntimeException | LinkageError e) {
                return true;
            }
        }
        int lastDot = className.lastIndexOf('.');
        String products = className.substring(0, lastDot + 1) + '$' + className.substring(lastDot + 1) + '$';
        for (BeanDefinitionReference<?> reference : beanContext.getBeanDefinitionReferences()) {
            String name = reference.getBeanDefinitionName();
            if (!name.startsWith(products) || !name.endsWith("$Definition")) {
                continue;
            }
            try {
                if (!isSerdeType(reference.getBeanType())) {
                    continue;
                }
                // the name alone matches a nested class too: the declaring type tells the product of this factory
                BeanDefinition<?> definition = reference.load();
                if (definition == null || definition.getDeclaringType().map(Class::getName).filter(className::equals).isPresent()) {
                    return true;
                }
            } catch (RuntimeException | LinkageError e) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSerdeType(Class<?> beanType) {
        for (Class<?> serdeType : SERDE_TYPES) {
            if (serdeType.isAssignableFrom(beanType)) {
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
     * Recreates what changed and the consumer processor, which stops every consumer. The context creates the
     * processor again at once and gives it the listener methods, as at startup, so that it starts the consumers again
     * on top of the new beans. Nothing is created that was not created already, but for the listener beans the
     * processor starts.
     *
     * @param typeCaches Whether to recreate the serde and producer registries, and the beans that received them
     * @param listeners The listener classes that changed, whose beans are recreated
     * @param reason Why, for the log
     */
    private void restartConsumers(boolean typeCaches, List<String> listeners, String reason) {
        if (!(beanContext instanceof WatchableBeanContext context)) {
            return;
        }
        // taken first: recreating one destroys the beans that received it, as the graph records them. The listener
        // beans go first, as the processor starts its consumers again as soon as it is recreated, on the listener
        // beans it finds then; it goes last, recreated already, and so skipped, when a registry it received was
        List<Object> beans = new ArrayList<>();
        if (!listeners.isEmpty()) {
            Set<String> names = new HashSet<>(listeners);
            for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(Qualifiers.byStereotype(KafkaListener.class))) {
                if (names.contains(registration.getBeanDefinition().getBeanType().getName())) {
                    add(beans, registration.bean());
                }
            }
        }
        if (typeCaches) {
            for (Class<?> type : TYPE_CACHES) {
                for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(type)) {
                    add(beans, registration.bean());
                }
            }
        }
        for (BeanRegistration<KafkaConsumerProcessor> registration : beanContext.getActiveBeanRegistrations(KafkaConsumerProcessor.class)) {
            add(beans, registration.bean());
        }
        if (beans.isEmpty()) {
            return;
        }
        LOG.debug("Restarting the Kafka consumers: {}", reason);
        for (Object bean : beans) {
            // false for a bean destroyed with one recreated before it, and for all of them in a context that does
            // not track bean dependencies: they are kept, and the consumers keep running until a restart
            context.recreate(bean);
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
     * Recreates the serde and producer registries when a serde, serializer, deserializer or serde registry definition
     * is registered or removed, as the in-place class change only sees the definitions that remain. The first batch
     * is what the registries were built from.
     */
    private final class SerdeDefinitionsWatcher implements BeanDefinitionWatcher<Object>, Ordered {
        @Override
        public void onChange(BeanDefinitionChange<Object> change) {
            if (!change.initial() && (!change.added().isEmpty() || !change.removed().isEmpty())) {
                restartConsumers(true, List.of(), "serde definitions changed");
            }
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

}
