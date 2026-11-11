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
import io.micronaut.configuration.kafka.admin.AdminClientFactory;
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.serde.CompositeSerdeRegistry;
import io.micronaut.configuration.kafka.serde.SerdeRegistry;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.Qualifier;
import io.micronaut.context.WatchableBeanContext;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.env.DevelopmentActive;
import io.micronaut.context.reload.BeanRetentionPolicy;
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
import io.micronaut.inject.BeanType;
import io.micronaut.inject.ExecutableMethod;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Keeps the Kafka consumers, producers and serdes in step with the code in development mode. It exists only in
 * development mode, so nothing of it is on the path of a record consumed or produced.
 *
 * <ul>
 *     <li>A {@link KafkaListener} bean definition registered or removed, the listener on the class or on a method, or a listener class changed in place,
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
 * <p>In development mode {@link AdminClientFactory} builds the admin client from a
 * {@link io.micronaut.configuration.kafka.config.RetainedKafkaConfiguration}, which receives nothing of the context,
 * and retains it across a restart with {@link io.micronaut.context.annotation.Retain}, until a change under
 * {@code kafka} releases it. This reloader refuses it while the configuration names a class of
 * the application, such as a metric reporter or a SASL callback handler, which the admin client would instantiate and
 * keep running from the retired generation. Neither the consumers, the producers nor the streams are retained: they
 * run the application's listeners and serdes.</p>
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
@DevelopmentActive
final class DevelopmentKafkaReloader implements BeanRetentionPolicy {

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

    /**
     * The listener beans: a {@link KafkaListener} or {@link Topic} on the class, or on an executable method.
     */
    private static final Qualifier<Object> LISTENERS = new ListenerQualifier();

    /**
     * The serde beans: a serializer, a deserializer, a serde or a serde registry.
     */
    private static final Qualifier<Object> SERDES = new SerdeQualifier();

    /**
     * The properties under {@code kafka} that are not those of the admin client, as {@code KafkaDefaultConfiguration}
     * leaves them out.
     */
    private static final List<String> NOT_ADMIN_PROPERTIES = List.of("embedded", "consumers", "producers", "streams");

    /**
     * A fully qualified class name: a value of the configuration that Kafka may load as a class.
     */
    private static final Pattern CLASS_NAME = Pattern.compile("(?:[\\p{L}_$][\\p{L}\\p{N}_$]*\\.)+[\\p{L}_$][\\p{L}\\p{N}_$]*");

    private final BeanContext beanContext;

    /**
     * @param beanContext The context, watched when it can be
     */
    DevelopmentKafkaReloader(BeanContext beanContext) {
        this.beanContext = beanContext;
        if (beanContext instanceof WatchableBeanContext watchable) {
            watchable.definitions().qualifier(LISTENERS).watch(new ListenerDefinitionsWatcher());
            // one watch for every serde type, so that a definition of several of them restarts the consumers once
            watchable.definitions().qualifier(SERDES).watch(new SerdeDefinitionsWatcher());
            watchable.classChanges().watch(new ClassWatcher());
        }
    }

    /**
     * Refuses the admin client of {@link AdminClientFactory}, which it retains, when the configuration names a class of
     * the application.
     *
     * @param registration The bean's registration
     * @return {@link Decision#REFUSE} for the admin client while the configuration names a class of the application
     */
    @Override
    public Decision decide(BeanRegistration<?> registration) {
        return isAdminClient(registration) && namesApplicationClass() ? Decision.REFUSE : Decision.ABSTAIN;
    }

    private static boolean isAdminClient(BeanRegistration<?> registration) {
        BeanDefinition<?> definition = registration.getBeanDefinition();
        return definition.getBeanType() == AdminClient.class
            && definition.getDeclaringType().filter(type -> type == AdminClientFactory.class).isPresent();
    }

    /**
     * Whether a property of the admin client's configuration names a class the application defines, rather than the
     * classpath Kafka is loaded from: the admin client would instantiate it and keep it running, and the retired
     * generation reachable, after the restart replaced it.
     */
    private boolean namesApplicationClass() {
        if (!(beanContext instanceof ApplicationContext applicationContext)) {
            return false;
        }
        Map<String, Object> properties = applicationContext.getEnvironment().getProperties(AbstractKafkaConfiguration.PREFIX);
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            String key = property.getKey();
            if (NOT_ADMIN_PROPERTIES.stream().noneMatch(key::startsWith) && namesApplicationClass(property.getValue())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("The Kafka admin client is not retained across the restart: {}.{} names a class of the application",
                        AbstractKafkaConfiguration.PREFIX, key);
                }
                return true;
            }
        }
        return false;
    }

    private boolean namesApplicationClass(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Class<?> type) {
            return isApplicationClass(type);
        }
        if (value instanceof Collection<?> values) {
            return values.stream().anyMatch(this::namesApplicationClass);
        }
        if (value.getClass().isArray()) {
            for (int i = 0; i < Array.getLength(value); i++) {
                if (namesApplicationClass(Array.get(value, i))) {
                    return true;
                }
            }
            return false;
        }
        // a list of class names, or a JAAS configuration that names a login module
        for (String token : value.toString().split("[\\s,;=\"']+")) {
            if (CLASS_NAME.matcher(token).matches() && isApplicationClass(load(token, beanContext.getClassLoader()))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a class was loaded by neither the classloader of the Kafka clients nor one of its parents.
     */
    private static boolean isApplicationClass(Class<?> type) {
        if (type == null) {
            return false;
        }
        ClassLoader loader = type.getClassLoader();
        if (loader == null) {
            return false;
        }
        for (ClassLoader kafka = AdminClient.class.getClassLoader(); kafka != null; kafka = kafka.getParent()) {
            if (kafka == loader) {
                return false;
            }
        }
        return true;
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
            if (isListener(type)) {
                listeners.add(className);
            }
            if (isSerdeOrClient(type)) {
                typeCaches = true;
            }
        }
        // what the beans were as the context was compiled: a change applied in place leaves the definitions as they were
        for (BeanDefinition<?> definition : changedDefinitions(change)) {
            String beanType = definition.getBeanType().getName();
            if (declares(definition, KafkaListener.class) && !listeners.contains(beanType)) {
                listeners.add(beanType);
            }
            if (isSerdeType(definition.getBeanType()) || declares(definition, KafkaClient.class)) {
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
     * The definitions of the context built from a class of a change applied in place: its bean type or a superclass
     * of it, the factory that declares it, or the class its advice intercepts, as the event tells.
     *
     * @param change The change
     * @return The definitions
     */
    private List<BeanDefinition<?>> changedDefinitions(ClassChangeEvent change) {
        List<BeanDefinition<?>> changed = new ArrayList<>();
        for (BeanDefinition<?> definition : beanContext.getAllBeanDefinitions()) {
            if (change.affectsDefinition(definition)) {
                changed.add(definition);
            }
        }
        return changed;
    }

    /**
     * Whether a definition carries a stereotype, on the class or an executable method.
     *
     * @param definition The definition
     * @param stereotype The stereotype
     * @return Whether it is there
     */
    private static boolean declares(BeanDefinition<?> definition, Class<? extends Annotation> stereotype) {
        try {
            if (definition.getAnnotationMetadata().hasStereotype(stereotype)) {
                return true;
            }
            for (ExecutableMethod<?, ?> method : definition.getExecutableMethods()) {
                if (method.getAnnotationMetadata().hasStereotype(stereotype)) {
                    return true;
                }
            }
            return false;
        } catch (RuntimeException | LinkageError e) {
            // a method that no longer resolves: what it was is unknown, so it counts
            return true;
        }
    }

    private static boolean isSerdeType(Class<?> beanType) {
        for (Class<?> serdeType : SERDE_TYPES) {
            if (serdeType.isAssignableFrom(beanType)) {
                return true;
            }
        }
        return false;
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
            for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(LISTENERS)) {
                if (names.contains(registration.getBeanDefinition().getBeanType().getName())) {
                    add(beans, registration.bean());
                }
            }
        }
        if (typeCaches) {
            // the composite received the other serde registries: the first of them destroys it and the processor, and
            // recreating it before them would restart the consumers once more. Each further custom registry still
            // does, as the context recreates one bean at a time and has no way to recreate them as a batch
            for (BeanRegistration<SerdeRegistry> registration : beanContext.getActiveBeanRegistrations(SerdeRegistry.class)) {
                if (!(registration.bean() instanceof CompositeSerdeRegistry)) {
                    add(beans, registration.bean());
                }
            }
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
     * Selects the listener beans, whether {@link KafkaListener} or {@link Topic} is on the class or only on a method.
     * The class metadata of a bean is read first, which needs nothing loaded. The methods are read only for a bean that
     * requires method processing, as a bean whose methods the {@link KafkaConsumerProcessor} receives does; that skips
     * the executable methods of every other bean, and the class that holds them. A reference, which has no methods to
     * read, is kept when it requires method processing.
     */
    private static final class ListenerQualifier implements Qualifier<Object> {
        @Override
        public <B extends BeanType<Object>> Stream<B> reduce(Class<Object> beanType, Stream<B> candidates) {
            return candidates.filter(ListenerQualifier::isListenerBean);
        }

        @Override
        public boolean doesQualify(Class<Object> beanType, BeanType<Object> candidate) {
            return isListenerBean(candidate);
        }

        private static boolean isListenerBean(BeanType<?> candidate) {
            AnnotationMetadata metadata = candidate.getAnnotationMetadata();
            if (metadata.hasStereotype(KafkaListener.class) || metadata.hasStereotype(Topic.class)) {
                return true;
            }
            if (!candidate.requiresMethodProcessing()) {
                return false;
            }
            if (!(candidate instanceof BeanDefinition<?> definition)) {
                return true;
            }
            for (ExecutableMethod<?, ?> method : definition.getExecutableMethods()) {
                AnnotationMetadata methodMetadata = method.getAnnotationMetadata();
                if (methodMetadata.hasStereotype(KafkaListener.class) || methodMetadata.hasStereotype(Topic.class)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return "Kafka listeners";
        }
    }

    /**
     * Selects the beans of any of the {@link #SERDE_TYPES}, by the bean type.
     */
    private static final class SerdeQualifier implements Qualifier<Object> {
        @Override
        public <B extends BeanType<Object>> Stream<B> reduce(Class<Object> beanType, Stream<B> candidates) {
            return candidates.filter(SerdeQualifier::isSerdeBean);
        }

        @Override
        public boolean doesQualify(Class<Object> beanType, BeanType<Object> candidate) {
            return isSerdeBean(candidate);
        }

        private static boolean isSerdeBean(BeanType<?> candidate) {
            return isSerdeType(candidate.getBeanType());
        }

        @Override
        public String toString() {
            return "Kafka serdes";
        }
    }

    /**
     * Restarts the consumers when a listener definition is registered or removed, the listener being on the class or
     * on a method. The first batch is what the
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
