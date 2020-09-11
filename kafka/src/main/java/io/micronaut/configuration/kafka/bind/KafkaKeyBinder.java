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

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.inject.Singleton;
import java.util.Optional;

/**
 * Binder for binding the parameters that is designated the {@link KafkaKey}.
 *
 * @param <T> The target type
 * @author Graeme Rocher
 * @since 1.0
 */
@Singleton
public class KafkaKeyBinder<T> implements AnnotatedConsumerRecordBinder<KafkaKey, T> {

    @Override
    public Class<KafkaKey> annotationType() {
        return KafkaKey.class;
    }

    @Override
    public BindingResult<T> bind(ArgumentConversionContext<T> context, ConsumerRecord<?, ?> source) {
        Object key = source.key();
        Optional<T> converted = ConversionService.SHARED.convert(key, context);
        return () -> converted;
    }
}
