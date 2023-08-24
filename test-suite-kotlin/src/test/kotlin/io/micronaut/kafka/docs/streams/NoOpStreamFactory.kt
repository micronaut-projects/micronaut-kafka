package io.micronaut.kafka.docs.streams

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.apache.kafka.streams.kstream.KStream

@Factory
class NoOpStreamFactory {
    @Singleton
    @Named("no-op-stream")
    fun noOpStream(builder: ConfiguredStreamBuilder): KStream<String, String> {
        return builder.stream("no-op-input")
    }
}
