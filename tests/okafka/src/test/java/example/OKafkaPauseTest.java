package example;

import io.micronaut.configuration.kafka.ConsumerFactory;
import io.micronaut.configuration.kafka.ConsumerRegistry;
import io.micronaut.configuration.kafka.DefaultConsumerFactory;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.producer.KafkaProducer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An OKafka consumer cannot pause, and its seek does not deliver again the records it already returned, so a paused
 * consumer must not fetch any.
 */
class OKafkaPauseTest {

    private static final String TOPIC = "OKAFKA_PAUSE";
    private static final String CONSUMER = "okafka-pause";
    private static final TopicPartition PARTITION_0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition(TOPIC, 1);

    private static OracleContainer oracle;
    private static ApplicationContext context;
    private static Properties clientProperties;

    @BeforeAll
    static void start() throws Exception {
        oracle = new OracleContainer("gvenzl/oracle-free:23.26.3-slim-faststart")
            .withCopyFileToContainer(MountableFile.forClasspathResource("oracle/okafka.sql"), "/container-entrypoint-startdb.d/okafka.sql");
        oracle.start();

        String tnsAdmin = writeOjdbcProperties().toString();
        String bootstrapServers = oracle.getHost() + ":" + oracle.getOraclePort();
        clientProperties = new Properties();
        clientProperties.setProperty("bootstrap.servers", bootstrapServers);
        clientProperties.setProperty("security.protocol", "PLAINTEXT");
        clientProperties.setProperty("oracle.service.name", oracle.getDatabaseName());
        clientProperties.setProperty("oracle.net.tns_admin", tnsAdmin);

        // the topic of the listener exists before the listener subscribes to it
        try (Admin admin = org.oracle.okafka.clients.admin.AdminClient.create(clientProperties)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1))).all().get();
        }

        context = ApplicationContext.run(Map.of(
            "spec.name", "OKafkaPauseTest",
            "okafka.tns-admin", tnsAdmin,
            "kafka.bootstrap.servers", bootstrapServers,
            "kafka.security.protocol", "PLAINTEXT",
            "kafka.oracle.service.name", oracle.getDatabaseName(),
            "kafka.health.enabled", false
        ));
    }

    @AfterAll
    static void stop() {
        if (context != null) {
            context.close();
        }
        if (oracle != null) {
            oracle.stop();
        }
    }

    @Test
    void aPausedConsumerFetchesNothingAndLosesNoRecord() throws Exception {
        ConsumerRegistry registry = context.getBean(ConsumerRegistry.class);
        PauseListener listener = context.getBean(PauseListener.class);

        try (Producer<String, String> producer = new KafkaProducer<>(clientProperties, new StringSerializer(), new StringSerializer())) {
            // OKafka only lists a partition in its assignment once records have been sent to it
            producer.send(new ProducerRecord<>(TOPIC, 0, "key", "before-0")).get();
            producer.send(new ProducerRecord<>(TOPIC, 1, "key", "before")).get();
            producer.flush();
            await().atMost(120, SECONDS).until(() -> listener.received.contains("before") && listener.received.contains("before-0"));
            await().atMost(60, SECONDS).ignoreException(IllegalArgumentException.class).until(() -> registry.getConsumerAssignment(CONSUMER).size() == 2);
            listener.received.remove("before-0");

            registry.pause(CONSUMER, List.of(PARTITION_0));
            await().atMost(30, SECONDS).until(() -> registry.isPaused(CONSUMER, List.of(PARTITION_0)));
            assertTrue(!registry.isPaused(CONSUMER, List.of(PARTITION_1)));

            producer.send(new ProducerRecord<>(TOPIC, 0, "key", "paused-1")).get();
            producer.send(new ProducerRecord<>(TOPIC, 0, "key", "paused-2")).get();
            producer.send(new ProducerRecord<>(TOPIC, 1, "key", "other")).get();
            producer.flush();

            // a consumer that cannot pause does not poll at all, so the other partition waits too
            Thread.sleep(SECONDS.toMillis(15));
            assertEquals(List.of("before"), listener.received);
            assertTrue(registry.isPaused(CONSUMER, List.of(PARTITION_0)));

            registry.resume(CONSUMER, List.of(PARTITION_0));
            await().atMost(60, SECONDS).until(() -> listener.received.size() == 4);
        }

        assertEquals(List.of("paused-1", "paused-2"), listener.received.stream().filter(value -> value.startsWith("paused")).toList());
        assertTrue(listener.received.contains("other"));
    }

    private static Path writeOjdbcProperties() throws IOException {
        Path directory = Files.createTempDirectory("okafka");
        Files.writeString(directory.resolve("ojdbc.properties"), "user=" + oracle.getUsername() + "\npassword=" + oracle.getPassword() + "\n");
        return directory;
    }

    @Requires(property = "spec.name", value = "OKafkaPauseTest")
    @KafkaListener(clientId = CONSUMER, groupId = "OKAFKA_PAUSE_GROUP", offsetReset = OffsetReset.EARLIEST)
    static class PauseListener {

        final List<String> received = new CopyOnWriteArrayList<>();

        @Topic(TOPIC)
        void receive(String value) {
            received.add(value);
        }
    }

    @Singleton
    @Replaces(DefaultConsumerFactory.class)
    @Requires(property = "spec.name", value = "OKafkaPauseTest")
    static class OKafkaConsumerFactory implements ConsumerFactory {

        private final String tnsAdmin;

        OKafkaConsumerFactory(@Value("${okafka.tns-admin}") String tnsAdmin) {
            this.tnsAdmin = tnsAdmin;
        }

        @Override
        public <K, V> Consumer<K, V> createConsumer(Properties config, @Nullable Deserializer<K> kd, @Nullable Deserializer<V> vd) {
            Properties properties = new Properties();
            properties.putAll(config);
            properties.setProperty("oracle.net.tns_admin", tnsAdmin);
            return new KafkaConsumer<>(properties, kd, vd);
        }
    }
}
