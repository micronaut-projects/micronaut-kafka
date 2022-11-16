/*
 * Copyright 2017-2021 original authors
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
package io.micronaut.configuration.kafka.bind;

import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.bind.ArgumentBinder;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.messaging.annotation.MessageHeader;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Iterator;
import java.util.Optional;

/**
 * Binds message headers.
 * @param <T> The generic type
 */
@Singleton
public class KafkaMessageHeaderBinder<T> implements AnnotatedConsumerRecordBinder<MessageHeader, T> {
    public static final BindingResult<?> EMPTY_OPTIONAL = () -> Optional.of(Optional.empty());
    private final ConversionService conversionService;

    public KafkaMessageHeaderBinder(ConversionService conversionService) {
        this.conversionService = conversionService;
    }

    @Override
    public Class<MessageHeader> annotationType() {
        return MessageHeader.class;
    }

    @Override
    public BindingResult<T> bind(ArgumentConversionContext<T> context, ConsumerRecord<?, ?> source) {
        Headers headers = source.headers();
        AnnotationMetadata annotationMetadata = context.getAnnotationMetadata();

        // use deprecated versions as that is what is stored in metadata
        Argument<T> argument = context.getArgument();
        String name = annotationMetadata.stringValue(MessageHeader.class, "name")
                .orElseGet(() -> annotationMetadata.stringValue(MessageHeader.class)
                        .orElse(argument.getName()));
        Iterator<Header> i = headers.headers(name).iterator();
        if (i.hasNext()) {
            Header value = i.next();
            if (argument.isOptional()) {
                Argument<?> typeVar = argument.getFirstTypeVariable().orElse(argument);
                Optional<?> converted = conversionService.convert(value.value(), typeVar);
                //noinspection unchecked
                return () -> (Optional<T>) Optional.of(converted);
            } else if (value != null) {
                Optional<T> converted = conversionService.convert(value.value(), context);
                return () -> converted;
            }
        }

        if (argument.isOptional()) {
            //noinspection unchecked
            return (BindingResult<T>) EMPTY_OPTIONAL;
        } else {
            if ("topic".equalsIgnoreCase(name)) {
                Optional<T> converted = conversionService.convert(source.topic(), context);
                return () -> converted;
            } else {
                //noinspection unchecked
                return BindingResult.EMPTY;
            }
        }
    }
}
