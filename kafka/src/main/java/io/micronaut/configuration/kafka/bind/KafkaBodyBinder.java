/*
 * Copyright 2017-2020 original authors
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

import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.http.annotation.Body;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.inject.Singleton;
import java.util.Optional;

/**
 * The default binder that binds the body of a ConsumerRecord.
 *
 * @param <T> The target generic type
 * @author Graeme Rocher
 * @since 1.0
 */
@Singleton
public class KafkaBodyBinder<T> implements AnnotatedConsumerRecordBinder<Body, T> {

    @Override
    public Class<Body> annotationType() {
        return Body.class;
    }

    @Override
    public BindingResult<T> bind(ArgumentConversionContext<T> context, ConsumerRecord<?, ?> source) {
        Object value = source.value();
        Optional<T> converted = ConversionService.SHARED.convert(value, context);
        return () -> converted;
    }
}
