/*
 * Copyright 2017-2026 original authors
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
package io.micronaut.configuration.kafka.dev;

import io.micronaut.context.ApplicationContext;
import io.micronaut.dev.tck.ReloadHarness;
import io.micronaut.dev.tck.ReloadTck;
import io.micronaut.testcontainers.kafka.Kafka;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs an application with a Kafka listener through the development runtime, against a broker, and edits the
 * listener. The restart closes the consumers of the retired generation as its context stops: they leave the
 * consumer group at once, rather than when the session times out, and nothing of them keeps the retired
 * generation reachable. The admin client is retained across the restart, until a change under {@code kafka}
 * releases it.
 */
class KafkaReloadTest {

    private static final String TOPIC = "dev-reload-topic";
    private static final String GROUP = "dev-reload-group";

    private static final String LISTENER = """
        package example;

        import io.micronaut.configuration.kafka.annotation.KafkaListener;
        import io.micronaut.configuration.kafka.annotation.OffsetReset;
        import io.micronaut.configuration.kafka.annotation.Topic;

        import java.util.List;
        import java.util.concurrent.CopyOnWriteArrayList;

        @KafkaListener(groupId = "%s", offsetReset = OffsetReset.EARLIEST, sessionTimeout = "45s")
        public class Listener {
            private final List<String> received = new CopyOnWriteArrayList<>();

            @Topic("%s")
            public void receive(String value) {
                received.add("%s " + value);
            }

            public List<String> received() {
                return received;
            }
        }
        """;

    private static final String REPORTER = """
        package example;

        import org.apache.kafka.common.metrics.KafkaMetric;
        import org.apache.kafka.common.metrics.MetricsReporter;

        import java.util.List;
        import java.util.Map;

        public class Reporter implements MetricsReporter {
            @Override
            public void init(List<KafkaMetric> metrics) {
            }

            @Override
            public void metricChange(KafkaMetric metric) {
            }

            @Override
            public void metricRemoval(KafkaMetric metric) {
            }

            @Override
            public void close() {
            }

            @Override
            public void configure(Map<String, ?> configs) {
            }
        }
        """;

    @TempDir
    Path project;

    @Test
    void theRestartClosesTheConsumersOfTheRetiredGenerationAndLeavesItCollectable() throws Exception {
        String bootstrap = Kafka.getProperties().get("kafka.bootstrap.servers");
        try (ReloadHarness harness = ReloadHarness.inDirectory(project);
             Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
             KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap), new StringSerializer(), new StringSerializer())) {
            harness.property("kafka.bootstrap.servers", bootstrap);
            harness.property("kafka.health.enabled", "false");
            harness.source("example.Listener", LISTENER.formatted(GROUP, TOPIC, "first"));
            harness.start();
            assertReloaderPresent(harness.context());

            producer.send(new ProducerRecord<>(TOPIC, "one")).get();
            awaitTrue("the first generation consumes", () -> received(harness.context()).contains("first one"));
            awaitTrue("the first generation joined the group", () -> members(admin) == 1);
            ReloadTck.assertFollowsReload(harness, KafkaReloadTest::listener);
            AdminClient adminClient = harness.context().getBean(AdminClient.class);

            harness.source("example.Listener", LISTENER.formatted(GROUP, TOPIC, "second"));
            long reloadStart = System.nanoTime();
            harness.reload();
            assertEquals(2, harness.generation());
            assertReloaderPresent(harness.context());

            // the admin client, with its connections, is kept
            ReloadTck.assertRetained(harness, adminClient);
            assertSame(adminClient, harness.context().getBean(AdminClient.class));
            assertTopicsListed(adminClient);
            adminClient = null;

            // the retired consumer left the group as its context stopped: the group settles on the one member of the
            // new generation well before the 45 second session timeout would have evicted a consumer that did not leave
            awaitTrue("the group has the one member of the second generation", () -> members(admin) == 1 && consumerThreads() <= 1);
            long settled = Duration.ofNanos(System.nanoTime() - reloadStart).toMillis();
            assertTrue(settled < 30_000, "the group settled in " + settled + " ms");
            System.out.println("The consumer group settled on the second generation " + settled + " ms after the reload started");

            producer.send(new ProducerRecord<>(TOPIC, "two")).get();
            awaitTrue("the second generation consumes", () -> received(harness.context()).contains("second two"));
            assertFalse(received(harness.context()).contains("first two"));
            ReloadTck.assertFollowsReload(harness, KafkaReloadTest::listener);

            // neither the consumers of the first generation, their threads, the retained admin client nor the
            // development-only reloader keep it reachable
            ReloadTck.assertRetiredGenerationsCollected(harness);
        }
    }

    @Test
    void aChangeUnderTheKafkaPrefixReleasesAndClosesTheAdminClient() throws Exception {
        String bootstrap = Kafka.getProperties().get("kafka.bootstrap.servers");
        try (ReloadHarness harness = ReloadHarness.inDirectory(project)) {
            harness.property("kafka.bootstrap.servers", bootstrap);
            harness.property("kafka.health.enabled", "false");
            harness.source("example.Listener", LISTENER.formatted(GROUP + "-released", TOPIC, "first"));
            harness.start();
            AdminClient first = harness.context().getBean(AdminClient.class);
            assertTopicsListed(first);

            // the application properties change under kafka, together with a class, so the application restarts
            harness.resource("application.properties", """
                kafka.bootstrap.servers=%s
                kafka.health.enabled=false
                kafka.request.timeout.ms=20000
                """.formatted(bootstrap));
            harness.source("example.Listener", LISTENER.formatted(GROUP + "-released", TOPIC, "second"));
            harness.reload();
            assertEquals(2, harness.generation());

            AdminClient second = harness.context().getBean(AdminClient.class);
            assertNotSame(first, second, "a change under kafka releases the admin client");
            assertTopicsListed(second);
            // the released admin client was closed with the retired generation
            AdminClient released = first;
            ExecutionException closed = assertThrows(ExecutionException.class,
                () -> released.listTopics().names().get(10, TimeUnit.SECONDS));
            System.out.println("The released admin client refuses calls: " + closed.getCause());
            first = null;
            second = null;
            ReloadTck.assertRetiredGenerationsCollected(harness);
        }
    }

    @Test
    void anAdminClientConfiguredWithAClassOfTheApplicationIsNotRetained() throws Exception {
        String bootstrap = Kafka.getProperties().get("kafka.bootstrap.servers");
        try (ReloadHarness harness = ReloadHarness.inDirectory(project)) {
            harness.property("kafka.bootstrap.servers", bootstrap);
            harness.property("kafka.health.enabled", "false");
            // the admin client instantiates the reporter, a class of the application, and keeps it
            harness.property("kafka.metric.reporters", "example.Reporter");
            harness.source("example.Reporter", REPORTER);
            harness.source("example.Listener", LISTENER.formatted(GROUP + "-reporter", TOPIC, "first"));
            harness.start();
            AdminClient first = adminClientOfApplicationThread(harness);
            assertTopicsListed(first);

            harness.source("example.Listener", LISTENER.formatted(GROUP + "-reporter", TOPIC, "second"));
            harness.reload();
            assertEquals(2, harness.generation());

            AdminClient second = adminClientOfApplicationThread(harness);
            assertNotSame(first, second, "an admin client running a class of the retired generation is not retained");
            assertTopicsListed(second);
            first = null;
            second = null;
            // the admin client of the first generation, closed with it, keeps none of its reporters reachable
            ReloadTck.assertRetiredGenerationsCollected(harness);
        }
    }

    /**
     * Creates the admin client as a thread of the application would, with the generation's classloader as the
     * context classloader, through which Kafka loads the classes its configuration names.
     */
    private static AdminClient adminClientOfApplicationThread(ReloadHarness harness) {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(harness.context().getClassLoader());
        try {
            return harness.context().getBean(AdminClient.class);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    private static void assertTopicsListed(AdminClient adminClient) throws Exception {
        assertTrue(adminClient.listTopics().names().get(30, TimeUnit.SECONDS) != null);
    }

    private static int members(Admin admin) {
        try {
            ConsumerGroupDescription description = admin.describeConsumerGroups(List.of(GROUP)).describedGroups().get(GROUP).get();
            return description.members().size();
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * The live threads of Kafka consumers of the group: the heartbeat thread of a consumer that was not closed
     * would outlive its generation.
     */
    private static long consumerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(Thread::isAlive)
            .filter(thread -> thread.getName().contains(GROUP))
            .count();
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting until " + what);
            }
            Thread.sleep(100);
        }
    }

    private static void assertReloaderPresent(ApplicationContext context) {
        // the bean that follows changes in place exists in development mode only
        assertTrue(context.containsBean(type(context, "io.micronaut.configuration.kafka.processor.DevelopmentKafkaReloader")));
    }

    private static Object listener(ApplicationContext context) {
        return context.getBean(type(context, "example.Listener"));
    }

    @SuppressWarnings("unchecked")
    private static List<String> received(ApplicationContext context) {
        try {
            return (List<String>) type(context, "example.Listener").getMethod("received").invoke(listener(context));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Cannot read what the listener received", e);
        }
    }

    private static Class<?> type(ApplicationContext context, String className) {
        try {
            return Class.forName(className, true, context.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new AssertionError(className + " is not in the application", e);
        }
    }
}
