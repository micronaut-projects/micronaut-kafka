package io.micronaut.configuration.kafka.streams

import groovy.util.logging.Slf4j
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.StreamsConfig
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.file.Paths

@Slf4j
abstract class AbstractTestContainersSpec extends Specification {

    PollingConditions conditions = new PollingConditions(timeout: 60, delay: 1)

    @Shared @AutoCleanup EmbeddedServer embeddedServer
    @Shared @AutoCleanup ApplicationContext context
    @Shared static KafkaContainer kafkaContainer = KafkaSetup.init()

    def setupSpec() {
        List<Object> config = ["kafka.bootstrap.servers", kafkaContainer.bootstrapServers]
        config.addAll(getConfiguration())

        embeddedServer = ApplicationContext.run(EmbeddedServer, CollectionUtils.mapOf(config as Object[]))

        context = embeddedServer.getApplicationContext()
    }

    protected List<Object> getConfiguration() {
        return ['kafka.generic.config', "hello",
                'kafka.streams.my-stream.application.id', 'my-stream',
                'kafka.streams.my-stream.num.stream.threads', 10,
                'kafka.streams.optimization-on.application.id', 'optimization-on',
                'kafka.streams.optimization-on.topology.optimization', 'all',
                'kafka.streams.optimization-off.application.id', 'optimization-off',
                'kafka.streams.optimization-off.topology.optimization', 'none']
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
        log.warn("tmp {} path {}", tmpDir, path)
        if (path != null) {
            final File node = Paths.get(path).normalize().toFile();
            log.warn("File {}", node.getAbsolutePath())
            // Only purge state when it's under java.io.tmpdir.  This is a safety net to prevent accidentally
            // deleting important local directory trees.
            if (node.getAbsolutePath().startsWith(tmpDir)) {
                log.warn("Deleting state in {}", node.getAbsolutePath())
                Utils.delete(new File(node.getAbsolutePath()));
            }
        }
    }
}
