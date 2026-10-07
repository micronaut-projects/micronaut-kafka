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
import io.micronaut.configuration.kafka.serde.SerdeRegistry;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.Qualifier;
import io.micronaut.context.WatchableBeanContext;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.DevelopmentMode;
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
import io.micronaut.inject.BeanDefinitionReference;
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
 * <p>Across a restart it retains the admin client that {@link AdminClientFactory} creates, with its connections,
 * until a change under {@code kafka} releases it: the admin client copies the properties of the default configuration
 * as it is created, and the next context binds the configuration again. It is retained only while the configuration
 * names no class of the application, such as a metric reporter or a SASL callback handler, which the admin client
 * would instantiate and keep running from the retired generation. Neither the consumers, the producers nor the
 * streams are retained: they run the application's listeners and serdes.</p>
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
            watchable.watchDefinitions(Object.class, LISTENERS, new ListenerDefinitionsWatcher());
            // one watch for every serde type, so that a definition of several of them restarts the consumers once
            watchable.watchDefinitions(Object.class, SERDES, new SerdeDefinitionsWatcher());
            watchable.watchClassChanges(new ClassWatcher());
        }
    }

    /**
     * Retains the admin client of {@link AdminClientFactory} across a restart, unless the configuration names a class
     * of the application.
     *
     * @param registration The bean's registration
     * @return Whether it is the admin client, safe to retain
     */
    @Override
    public boolean retain(BeanRegistration<?> registration) {
        return isAdminClient(registration) && !namesApplicationClass();
    }

    /**
     * @param registration The retained bean's registration
     * @return {@code kafka}, under which the admin client's configuration is bound
     */
    @Override
    public Set<String> observedConfigurationPrefixes(BeanRegistration<?> registration) {
        return isAdminClient(registration) ? Set.of(AbstractKafkaConfiguration.PREFIX) : Set.of();
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
            for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(LISTENERS)) {
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
