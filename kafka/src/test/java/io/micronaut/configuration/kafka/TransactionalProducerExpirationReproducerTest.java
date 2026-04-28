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

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.type.Argument;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class TransactionalProducerExpirationReproducerTest {
    private static final String CLIENT_ID = "issue-542";
    private static final int TRANSACTIONAL_ID_EXPIRATION_MS = 3_000;
    private static final int TRANSACTION_CLEANUP_INTERVAL_MS = 500;
    private static final String TRANSACTIONAL_ID = "issue-542-tx";

    @Test
    @Timeout(180)
    void transactionalProducerShouldRecoverAfterTransactionalIdExpires() throws Exception {
        String topic = "issue-542-topic-" + UUID.randomUUID();
        String transactionalId = TRANSACTIONAL_ID + "-registry-" + UUID.randomUUID();
        try (KafkaContainer kafka = createKafkaContainer()) {
            kafka.start();
            try (ApplicationContext context = ApplicationContext.run(Map.of(
                "kafka.bootstrap.servers", kafka.getBootstrapServers()
            ))) {
                createTopic(kafka.getBootstrapServers(), topic);

                TransactionalProducerRegistry registry = context.getBean(TransactionalProducerRegistry.class);
                Producer<String, String> producer = registry.getTransactionalProducer(
                    CLIENT_ID,
                    transactionalId,
                    Argument.of(String.class),
                    Argument.of(String.class)
                );

                sendTransaction(producer, topic, "first");
                waitForTransactionalIdExpiration(kafka.getBootstrapServers(), transactionalId);

                assertDoesNotThrow(() -> sendTransaction(producer, topic, "second"));
            }
        }
    }

    @Test
    @Timeout(180)
    void injectedTransactionalProducerShouldRecoverAfterTransactionalIdExpires() throws Exception {
        String topic = "issue-542-topic-" + UUID.randomUUID();
        try (KafkaContainer kafka = createKafkaContainer()) {
            kafka.start();
            try (ApplicationContext context = ApplicationContext.run(Map.of(
                "kafka.bootstrap.servers", kafka.getBootstrapServers()
            ))) {
                createTopic(kafka.getBootstrapServers(), topic);

                InjectedTransactionalSender sender = context.getBean(InjectedTransactionalSender.class);
                sender.send(topic, "first").get();
                waitForTransactionalIdExpiration(kafka.getBootstrapServers(), TRANSACTIONAL_ID);

                assertDoesNotThrow(() -> sender.send(topic, "second").get());
            }
        }
    }

    private static void sendTransaction(Producer<String, String> producer, String topic, String value) throws Exception {
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(topic, value)).get();
        producer.commitTransaction();
    }

    private static KafkaContainer createKafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.0"))
            .withEnv("KAFKA_TRANSACTIONAL_ID_EXPIRATION_MS", Integer.toString(TRANSACTIONAL_ID_EXPIRATION_MS))
            .withEnv("KAFKA_TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS", Integer.toString(TRANSACTION_CLEANUP_INTERVAL_MS))
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }

    private static void waitForTransactionalIdExpiration(String bootstrapServers, String transactionalId) {
        try (AdminClient admin = AdminClient.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ))) {
            await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(TRANSACTION_CLEANUP_INTERVAL_MS))
                .until(() -> {
                    try {
                        admin.describeTransactions(List.of(transactionalId))
                            .description(transactionalId)
                            .get(2, TimeUnit.SECONDS);
                        return false;
                    } catch (ExecutionException e) {
                        return e.getCause() instanceof TransactionalIdNotFoundException;
                    } catch (Exception e) {
                        return false;
                    }
                });
        }
    }

    private static void createTopic(String bootstrapServers, String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    @Singleton
    static final class InjectedTransactionalSender {
        private final Producer<String, String> producer;

        InjectedTransactionalSender(@KafkaClient(id = CLIENT_ID, transactionalId = TRANSACTIONAL_ID) Producer<String, String> producer) {
            this.producer = producer;
            this.producer.initTransactions();
        }

        java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(String topic, String value) {
            producer.beginTransaction();
            java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> future =
                producer.send(new ProducerRecord<>(topic, value));
            producer.commitTransaction();
            return future;
        }
    }
}
