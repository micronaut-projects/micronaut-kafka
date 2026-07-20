package io.micronaut.configuration.kafka.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.inject.qualifiers.Qualifiers
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.Materialized
import spock.lang.AutoCleanup
import spock.lang.Shared

class GlobalKTableOnlySpec extends AbstractKafkaSpec {

    private static final String UNIQUE_SUFFIX = UUID.randomUUID()
    private static final String TMP_DIR = System.getProperty('java.io.tmpdir')

    @Shared
    @AutoCleanup
    ApplicationContext context = ApplicationContext.run(configuration)

    void "test global ktable only topology is registered"() {
        when:
        ConfiguredStreamBuilder builder = context.getBean(ConfiguredStreamBuilder, Qualifiers.byName(GlobalTableOnlyFactory.STREAM_NAME))
        context.getBean(GlobalKTable, Qualifiers.byName(GlobalTableOnlyFactory.STREAM_NAME))
        KafkaStreams kafkaStreams = context.getBean(KafkaStreams, Qualifiers.byName(GlobalTableOnlyFactory.STREAM_NAME))
        String topologyDescription = builder.build(builder.configuration).describe().toString()

        then:
        kafkaStreams
        topologyDescription.contains(GlobalTableOnlyFactory.INPUT)
        topologyDescription.contains(GlobalTableOnlyFactory.STORE)
    }

    void "global kafka streams enabled property is not treated as a stream name"() {
        given:
        ApplicationContext enabledContext = ApplicationContext.run([
                'spec.name'                                           : 'GlobalKTableOnlySpec',
                'kafka.test.initializer.enabled'                      : false,
                'kafka.bootstrap.servers'                             : 'localhost:9092',
                'kafka.streams.enabled'                               : true,
                'kafka.streams.default.application.id'                : 'default-enabled-' + UNIQUE_SUFFIX,
                'kafka.streams.default.start-kafka-streams'           : false,
                'kafka.streams.default.state.dir'                     : TMP_DIR + '/enabled-property-default-' + UNIQUE_SUFFIX,
                'kafka.streams.global-table-only.application.id'      : 'global-table-only-' + UNIQUE_SUFFIX,
                'kafka.streams.global-table-only.start-kafka-streams' : false,
                'kafka.streams.global-table-only.state.dir'           : TMP_DIR + '/enabled-property-global-' + UNIQUE_SUFFIX
        ])

        when:
        List<String> streamNames = enabledContext.getBeansOfType(ConfiguredStreamBuilder)*.name.sort()

        then:
        !streamNames.contains('enabled')

        cleanup:
        enabledContext.close()
    }

    @Override
    protected Map<String, Object> getConfiguration() {
        super.getConfiguration() + [
                'kafka.bootstrap.servers': 'localhost:9092',
                'kafka.test.initializer.enabled': 'false',
                'kafka.streams.default.application.id': 'default-' + UNIQUE_SUFFIX,
                'kafka.streams.default.start-kafka-streams': 'false',
                'kafka.streams.default.state.dir': TMP_DIR + '/global-ktable-default-' + UNIQUE_SUFFIX,
                'kafka.streams.my-stream.start-kafka-streams': 'false',
                'kafka.streams.optimization-on.start-kafka-streams': 'false',
                'kafka.streams.optimization-off.start-kafka-streams': 'false',
                'kafka.streams.start-kafka-streams-off.start-kafka-streams': 'false',
                'kafka.streams.global-table-only.application.id': 'global-table-only-' + UNIQUE_SUFFIX,
                'kafka.streams.global-table-only.start-kafka-streams': 'false',
                'kafka.streams.global-table-only.state.dir': TMP_DIR + '/global-ktable-only-' + UNIQUE_SUFFIX
        ]
    }

    @Requires(property = "spec.name", value = "GlobalKTableOnlySpec")
    @Factory
    static class GlobalTableOnlyFactory {
        static final String STREAM_NAME = "global-table-only"
        static final String INPUT = "global-table-only-input"
        static final String STORE = "global-table-only-store"

        @Singleton
        @Context
        @Named(STREAM_NAME)
        GlobalKTable<String, String> globalTable(@Named(STREAM_NAME) ConfiguredStreamBuilder builder) {
            builder.configuration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().class.name)
            builder.configuration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().class.name)
            builder.globalTable(INPUT, Materialized.as(STORE))
        }
    }
}
