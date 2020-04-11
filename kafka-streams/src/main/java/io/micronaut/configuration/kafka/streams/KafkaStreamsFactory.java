/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.streams;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;

import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;


/**
 * A factory that constructs the {@link KafkaStreams} bean.
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Factory
public class KafkaStreamsFactory implements Closeable {

    private final Collection<KafkaStreams> streams = new ConcurrentLinkedDeque<>();

    /**
     * Exposes the {@link ConfiguredStreamBuilder} as a bean.
     *
     * @param configuration The configuration
     * @return The streams builder
     */
    @EachBean(AbstractKafkaStreamsConfiguration.class)
    ConfiguredStreamBuilder streamsBuilder(AbstractKafkaStreamsConfiguration configuration) {
        return new ConfiguredStreamBuilder(configuration.getConfig());
    }

    /**
     * Builds the default {@link KafkaStreams} bean from the configuration and the supplied {@link ConfiguredStreamBuilder}.
     *
     * @param builder  The builder
     * @param kStreams The KStream definitions
     * @return The {@link KafkaStreams} bean
     */
    @EachBean(ConfiguredStreamBuilder.class)
    @Context
    KafkaStreams kafkaStreams(
            ConfiguredStreamBuilder builder,
            // required for initialization. DO NOT DELETE
            List<BeforeStartKafkaStreamsListener> listeners,
            KStream... kStreams
    ) {
        KafkaStreams kafkaStreams = new KafkaStreams(
                builder.build(builder.getConfiguration()),
                builder.getConfiguration()
        );
        for (BeforeStartKafkaStreamsListener listener : listeners) {
            listener.execute(kafkaStreams, kStreams);
        }
        streams.add(kafkaStreams);
        kafkaStreams.start();
        return kafkaStreams;
    }

    /**
     * Create the interactive query service bean.
     *
     * @return Rhe {@link InteractiveQueryService} bean
     */
    @Singleton
    InteractiveQueryService interactiveQueryService() {
        return new InteractiveQueryService(streams);
    }

    @Override
    @PreDestroy
    public void close() {
        for (KafkaStreams stream : streams) {
            try {
                stream.close(Duration.ofSeconds(3));
            } catch (Exception e) {
                // ignore
            }
        }
    }

}
