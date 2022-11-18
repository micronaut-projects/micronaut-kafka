package io.micronaut.configuration.kafka.streams

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.streams.optimization.OptimizationStream
import io.micronaut.configuration.kafka.streams.startkafkastreams.StartKafkaStreamsOff
import io.micronaut.configuration.kafka.streams.wordcount.WordCountStream
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.StreamsConfig
import spock.lang.Shared

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

@Slf4j
abstract class AbstractTestContainersSpec extends AbstractEmbeddedServerSpec {

    @Shared
    String myStreamApplicationId = 'my-stream-' + UUID.randomUUID().toString()

    @Shared
    String optimizationOnApplicationId = 'optimization-on-' + UUID.randomUUID().toString()

    @Shared
    String optimizationOffApplicationId = 'optimization-off-' + UUID.randomUUID().toString()

    @Shared
    String startKafkaStreamsOffApplicationId = 'start-kafka-streams-off-' + UUID.randomUUID().toString()

    protected Map<String, Object> getConfiguration() {
        super.getConfiguration() + ['kafka.generic.config': "hello",
                                    'kafka.streams.my-stream.application.id': myStreamApplicationId,
                                    'kafka.streams.my-stream.num.stream.threads': 10,
                                    'kafka.streams.optimization-on.application.id': optimizationOnApplicationId,
                                    'kafka.streams.optimization-on.topology.optimization': 'all',
                                    'kafka.streams.optimization-off.application.id': optimizationOffApplicationId,
                                    'kafka.streams.optimization-off.topology.optimization': 'none',
                                    'kafka.streams.start-kafka-streams-off.application.id': startKafkaStreamsOffApplicationId]
    }

    @Override
    void afterKafkaStarted() {
        [
                WordCountStream.INPUT,
                WordCountStream.OUTPUT,
                WordCountStream.NAMED_WORD_COUNT_INPUT,
                WordCountStream.NAMED_WORD_COUNT_OUTPUT,
                StartKafkaStreamsOff.STREAMS_OFF_INPUT,
                StartKafkaStreamsOff.STREAMS_OFF_OUTPUT,
                OptimizationStream.OPTIMIZATION_ON_INPUT,
                OptimizationStream.OPTIMIZATION_OFF_INPUT
        ].forEach(topic -> {
            createTopic(topic.toString(), 1, 1)
        })
    }

    def cleanupSpec() {
        def kafkaStreamsFactory = context.getBean(KafkaStreamsFactory)
        kafkaStreamsFactory.getStreams().forEach((kafkaStream, configuredStreamBuilder) -> {
            kafkaStream.close()
            kafkaStream.cleanUp()
            purgeLocalStreamsState(configuredStreamBuilder.configuration)
        })
        try {
            embeddedServer.stop()
            log.warn("Stopped containers!")
        } catch (Exception ignore) {
            log.error("Could not stop containers")
        }
        embeddedServer?.close()
    }

    static def purgeLocalStreamsState(final streamsConfiguration) throws IOException {
        final String tmpDir = System.getProperty("java.io.tmpdir");
        final String path = streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        if (path != null) {
            def p = Paths.get(path)
            final File node = p.normalize().toFile();
            // Only purge state when it's under java.io.tmpdir.  This is a safety net to prevent accidentally
            // deleting important local directory trees.
            if (node.getAbsolutePath().startsWith(tmpDir)) {
                    Files.walk(p)
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
            }
        }
    }
}
