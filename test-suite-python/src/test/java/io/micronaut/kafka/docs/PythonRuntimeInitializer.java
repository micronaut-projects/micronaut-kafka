package io.micronaut.kafka.docs;

import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import org.graalvm.polyglot.Context;

/**
 * TODO(python): {@code @Executable(processOnStartup = true)} processors such as the Kafka consumer
 * processor run before the {@code @Context} beans are initialized, so a Python {@code @KafkaListener}
 * bean would be instantiated before the GraalPy runtime exists ("GraalPy context has not been
 * initialized"). Creating the GraalPy context when such a processor is created makes sure the runtime
 * is installed before the first listener bean is instantiated.
 */
@Singleton
public class PythonRuntimeInitializer implements BeanCreatedEventListener<ExecutableMethodProcessor<?>> {

    @Override
    public ExecutableMethodProcessor<?> onCreated(BeanCreatedEvent<ExecutableMethodProcessor<?>> event) {
        event.getSource().getBean(Context.class, Qualifiers.byName("python"));
        return event.getBean();
    }
}
