package io.micronaut.configuration.kafka.streams

import groovy.util.logging.Slf4j
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import spock.lang.Shared

import java.nio.file.AccessDeniedException
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
        return super.getConfiguration() + ['kafka.generic.config': "hello",
                                    'kafka.consumers.OptimizationListener.allow.auto.create.topics': false,
                                    'kafka.streams.my-stream.application.id': myStreamApplicationId,
                                    'kafka.streams.my-stream.num.stream.threads': 10,
                                    'kafka.streams.optimization-on.application.id': optimizationOnApplicationId,
                                    'kafka.streams.optimization-on.topology.optimization': 'all',
                                    'kafka.streams.optimization-off.application.id': optimizationOffApplicationId,
                                    'kafka.streams.optimization-off.topology.optimization': 'none',
                                    'kafka.streams.start-kafka-streams-off.application.id': startKafkaStreamsOffApplicationId]
    }

    void stopContext() {
        def kafkaStreamsFactory = context.getBean(KafkaStreamsFactory)
        try {
          embeddedServer.stop()
          log.warn("Stopped containers!")
        } catch (Exception ignore) {
            log.error("Could not stop containers")
        }
        log.warn("Closing leftover streams")
        kafkaStreamsFactory.getStreams().keySet().stream().filter(k -> !k.state().hasCompletedShutdown()).forEach(KafkaStreams::close)
        log.warn("Purging stream state")
        kafkaStreamsFactory.getStreams().keySet().forEach(kafkaStream -> purgeLocalStreamsState(kafkaStream.applicationConfigs))
        log.warn("Cleaning up streams")
        kafkaStreamsFactory.getStreams().keySet().forEach(KafkaStreams::cleanUp)
    }

    static def purgeLocalStreamsState(final StreamsConfig streamsConfiguration) throws IOException {
        final String tmpDir = System.getProperty("java.io.tmpdir")
        final String path = streamsConfiguration.getString(StreamsConfig.STATE_DIR_CONFIG)
        if (path != null) {
            def p = Paths.get(path)
            final File node = p.normalize().toFile()
            // Only purge state when it's under java.io.tmpdir.  This is a safety net to prevent accidentally
            // deleting important local directory trees.
            if (node.getAbsolutePath().startsWith(tmpDir) && node.exists()) {
                    log.warn("Purging tmp dir {}", node.getAbsolutePath())
                    try {
                        Files.walk(p)
                                .sorted(Comparator.reverseOrder())
                                .map(Path::toFile)
                                .forEach(File::delete)
                    } catch (AccessDeniedException e) {
                        // ignore failure, disk read-only
                    }
            }
        }
    }
}
