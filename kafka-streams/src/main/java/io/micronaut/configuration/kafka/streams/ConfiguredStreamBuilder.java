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
package io.micronaut.configuration.kafka.streams;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.naming.Named;
import org.apache.kafka.streams.StreamsBuilder;

import java.time.Duration;
import java.util.Properties;

/**
 * Extended version of {@link StreamsBuilder} that can be configured.
 *
 * @author graemerocher
 * @since 1.0
 */
public class ConfiguredStreamBuilder extends StreamsBuilder implements Named {

    private final Properties configuration = new Properties();

    private final String streamName;

    private final Duration closeTimeout;

    /**
     * Default constructor.
     *
     * @param configuration The configuration
     * @param streamName The logical name of the stream
     * @param closeTimeout The time to wait for the stream to shut down
     */
    public ConfiguredStreamBuilder(Properties configuration, String streamName, Duration closeTimeout) {
        this.configuration.putAll(configuration);
        this.streamName = streamName;
        this.closeTimeout = closeTimeout;
    }

    /**
     * The configuration. Can be mutated.
     *
     * @return The configuration
     */
    public @NonNull Properties getConfiguration() {
        return configuration;
    }

    /**
     * The logical name of the stream.
     *
     * @return the logical name of the stream
     */
    @Override
    public @NonNull String getName() {
        return this.streamName;
    }

    /**
     * The timeout to use when closing the stream.
     *
     * @return the timeout to use when closing the stream
     */
    public @NonNull Duration getCloseTimeout() {
        return closeTimeout;
    }
}
