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

import io.micronaut.configuration.kafka.annotation.KafkaPartition;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionService;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Optional;

/**
 * Binder for binding the parameters that is designated the {@link KafkaPartition}.
 *
 * @param <T> The target type
 * @author André Prata
 * @since 3.3.4
 */
@Singleton
public class KafkaPartitionBinder<T> implements AnnotatedConsumerRecordBinder<KafkaPartition, T> {
    private final ConversionService conversionService;

    /**
     * Default constructor.
     * @param conversionService The conversion service
     */
    public KafkaPartitionBinder(ConversionService conversionService) {
        this.conversionService = conversionService;
    }

    @Override
    public Class<KafkaPartition> annotationType() {
        return KafkaPartition.class;
    }

    @Override
    public BindingResult<T> bind(ArgumentConversionContext<T> context, ConsumerRecord<?, ?> source) {
        Object partition = source.partition();
        Optional<T> converted = conversionService.convert(partition, context);
        return () -> converted;
    }
}
