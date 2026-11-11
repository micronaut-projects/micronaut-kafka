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
package io.micronaut.configuration.kafka.config;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.DevelopmentActive;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.convert.format.MapFormat;
import io.micronaut.core.util.StringUtils;

import java.util.Map;
import java.util.Properties;

/**
 * The values under {@code kafka} that the admin client retained across a development restart is built from.
 *
 * <p>The client is built from this configuration rather than from {@link KafkaDefaultConfiguration}, which receives the
 * environment: development mode refuses to retain a bean whose configuration holds what is bound to the context. This
 * configuration receives the values as a property, which resolves the keys {@code Environment.getProperties} does, and
 * converts them with the {@link ConversionService#SHARED shared conversion service}, without the converters of the
 * application. A replacement of {@link KafkaDefaultConfiguration}, or a listener that changes it, does not reach it. It
 * is only exposed under its own type, so that nothing that asks for a Kafka configuration sees it.</p>
 *
 * @author graemerocher
 * @since 6.3.0
 */
@Internal
@DevelopmentActive
@Requires(property = AbstractKafkaConfiguration.PREFIX)
@Requires(property = AbstractKafkaConfiguration.PREFIX + ".enabled", notEquals = StringUtils.FALSE)
@ConfigurationProperties(AbstractKafkaConfiguration.PREFIX)
@Bean(typed = RetainedKafkaConfiguration.class)
public final class RetainedKafkaConfiguration {

    private final Properties config;

    /**
     * @param values The values under {@code kafka}
     */
    RetainedKafkaConfiguration(@Property(name = AbstractKafkaConfiguration.PREFIX)
                               @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
                               Map<String, Object> values) {
        this.config = KafkaDefaultConfiguration.defaultProperties(ConversionService.SHARED, values);
    }

    /**
     * @return The Kafka properties
     */
    public Properties getConfig() {
        return config;
    }
}
