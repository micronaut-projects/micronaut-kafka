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
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs an application with a Kafka listener through the development runtime, against a broker, and edits the
 * listener. The restart closes the consumers of the retired generation as its context stops: they leave the
 * consumer group at once, rather than when the session times out, and nothing of them keeps the retired
 * generation reachable.
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

            harness.source("example.Listener", LISTENER.formatted(GROUP, TOPIC, "second"));
            long reloadStart = System.nanoTime();
            harness.reload();
            assertEquals(2, harness.generation());
            assertReloaderPresent(harness.context());

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

            // neither the consumers of the first generation, their threads, nor the development-only reloader keep it reachable
            ReloadTck.assertRetiredGenerationsCollected(harness);
        }
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
