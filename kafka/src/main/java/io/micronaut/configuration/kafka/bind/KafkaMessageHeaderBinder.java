package io.micronaut.configuration.kafka.bind;

import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.messaging.annotation.MessageHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class KafkaMessageHeaderBinder<T> implements AnnotatedConsumerRecordBinder<MessageHeader, T> {

    @Override
    public Class<MessageHeader> annotationType() {
        return MessageHeader.class;
    }

    @Override
    public BindingResult<T> bind(ArgumentConversionContext<T> context, ConsumerRecord<?, ?> source) {
        Headers headers = source.headers();
        AnnotationMetadata annotationMetadata = context.getAnnotationMetadata();

        String name = annotationMetadata.stringValue(MessageHeader.class, "name")
                .orElseGet(() -> annotationMetadata.stringValue(MessageHeader.class)
                        .orElse(context.getArgument().getName()));
        Iterable<org.apache.kafka.common.header.Header> value = headers.headers(name);

        if (value.iterator().hasNext()) {
            Optional<T> converted = ConversionService.SHARED.convert(value, context);
            return () -> converted;
        } else if (context.getArgument().getType() == Optional.class) {
            //noinspection unchecked
            return () -> (Optional<T>) Optional.of(Optional.empty());
        } else {
            //noinspection unchecked
            return BindingResult.EMPTY;
        }
    }
}
