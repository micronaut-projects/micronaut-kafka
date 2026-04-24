/*
 * Copyright 2017-2024 original authors
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
package io.micronaut.configuration.kafka;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.type.Argument;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.tools.StorageTool;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TransactionalProducerExpirationReproducerTest {
    private static final String CLIENT_ID = "issue-542";
    private static final String TOPIC = "issue-542-topic";
    private static final String TRANSACTIONAL_ID = "issue-542-tx";

    @Test
    @Timeout(90)
    void transactionalProducerShouldRecoverAfterTransactionalIdExpires() throws Exception {
        try (EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(3_000, 500);
             ApplicationContext context = ApplicationContext.run(Map.of(
                 "kafka.bootstrap.servers", broker.bootstrapServers()
             ))) {
            broker.createTopic(TOPIC);

            TransactionalProducerRegistry registry = context.getBean(TransactionalProducerRegistry.class);
            Producer<String, String> producer = registry.getTransactionalProducer(
                CLIENT_ID,
                TRANSACTIONAL_ID,
                Argument.of(String.class),
                Argument.of(String.class)
            );

            sendTransaction(producer, "first");
            Thread.sleep(12_000);

            assertDoesNotThrow(() -> sendTransaction(producer, "second"));
        }
    }

    private static void sendTransaction(Producer<String, String> producer, String value) throws Exception {
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(TOPIC, value)).get();
        producer.commitTransaction();
    }

    private static final class EmbeddedKafkaBroker implements AutoCloseable {
        private final KafkaRaftServer server;
        private final Path baseDir;
        private final String bootstrapServers;

        private EmbeddedKafkaBroker(int expirationMs, int cleanupIntervalMs) throws Exception {
            int brokerPort = freePort();
            int controllerPort = freePort();
            baseDir = Files.createTempDirectory("issue-542-kafka-");
            Path dataDir = Files.createDirectory(baseDir.resolve("data"));
            Path metadataDir = Files.createDirectory(baseDir.resolve("metadata"));
            Path configFile = baseDir.resolve("server.properties");
            bootstrapServers = "127.0.0.1:" + brokerPort;

            Files.writeString(configFile, """
                process.roles=broker,controller
                node.id=1
                controller.quorum.voters=1@127.0.0.1:%d
                listeners=PLAINTEXT://127.0.0.1:%d,CONTROLLER://127.0.0.1:%d
                advertised.listeners=PLAINTEXT://127.0.0.1:%d
                listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
                inter.broker.listener.name=PLAINTEXT
                controller.listener.names=CONTROLLER
                log.dirs=%s
                metadata.log.dir=%s
                num.partitions=1
                offsets.topic.replication.factor=1
                transaction.state.log.replication.factor=1
                transaction.state.log.min.isr=1
                transaction.remove.expired.transaction.cleanup.interval.ms=%d
                group.initial.rebalance.delay.ms=0
                transactional.id.expiration.ms=%d
                auto.create.topics.enable=false
                """.formatted(
                controllerPort,
                brokerPort,
                controllerPort,
                brokerPort,
                dataDir,
                metadataDir,
                cleanupIntervalMs,
                expirationMs
            ));

            int formatExit = StorageTool.execute(
                new String[]{"format", "--config", configFile.toString(), "--cluster-id", Uuid.randomUuid().toString()},
                System.out
            );
            if (formatExit != 0) {
                throw new IllegalStateException("Kafka storage format failed with exit code " + formatExit);
            }

            server = new KafkaRaftServer(KafkaConfig.fromProps(load(configFile)), Time.SYSTEM);
            server.startup();
            waitForBroker();
        }

        private String bootstrapServers() {
            return bootstrapServers;
        }

        private void createTopic(String name) throws Exception {
            try (AdminClient admin = adminClient()) {
                admin.createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get();
            }
        }

        private void waitForBroker() throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            Exception last = null;
            while (System.nanoTime() < deadline) {
                try (AdminClient admin = adminClient()) {
                    admin.describeCluster().clusterId().get();
                    return;
                } catch (Exception e) {
                    last = e;
                    Thread.sleep(500);
                }
            }
            throw new IllegalStateException("Broker did not become ready", last);
        }

        private AdminClient adminClient() {
            return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
            ));
        }

        @Override
        public void close() throws Exception {
            try {
                server.shutdown();
                server.awaitShutdown();
            } finally {
                deleteRecursively(baseDir);
            }
        }

        private static Properties load(Path configFile) throws IOException {
            Properties properties = new Properties();
            try (var input = Files.newInputStream(configFile)) {
                properties.load(input);
            }
            return properties;
        }

        private static void deleteRecursively(Path path) throws IOException {
            if (!Files.exists(path)) {
                return;
            }
            try (var walk = Files.walk(path)) {
                walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(current -> {
                        try {
                            Files.deleteIfExists(current);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IOException ioException) {
                    throw ioException;
                }
                throw e;
            }
        }

        private static int freePort() throws IOException {
            try (ServerSocket socket = new ServerSocket(0)) {
                return socket.getLocalPort();
            }
        }
    }
}
