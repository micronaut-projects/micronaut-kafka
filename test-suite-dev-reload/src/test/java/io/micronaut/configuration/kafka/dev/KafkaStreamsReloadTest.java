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
import io.micronaut.context.DefaultBeanContext;
import io.micronaut.context.reload.ClassChange;
import io.micronaut.context.reload.ClassChangeEvent;
import io.micronaut.context.reload.ReloadStrategy;
import io.micronaut.dev.tck.ReloadHarness;
import io.micronaut.dev.tck.ReloadTck;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.testcontainers.kafka.Kafka;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs an application with a Kafka Streams topology through the development runtime, against a broker. A change of
 * the class that builds the topology, or of a bean it delegates to, applied in place rebuilds the streams; a restart closes the streams of the
 * retired generation as its context stops, and nothing of them keeps it reachable.
 */
class KafkaStreamsReloadTest {

    private static final String IN = "dev-streams-in";
    private static final String OUT = "dev-streams-out";
    private static final String STREAM = "upper";

    private static final String TOPOLOGY = """
        package example;

        import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
        import io.micronaut.context.annotation.Factory;
        import jakarta.inject.Named;
        import jakarta.inject.Singleton;
        import org.apache.kafka.common.serialization.Serdes;
        import org.apache.kafka.streams.StreamsConfig;
        import org.apache.kafka.streams.kstream.KStream;

        @Factory
        public class Topology {
            @Singleton
            @Named("%1$s")
            KStream<String, String> stream(@Named("%1$s") ConfiguredStreamBuilder builder, Decorator decorator) {
                builder.getConfiguration().put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
                builder.getConfiguration().put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
                KStream<String, String> source = builder.stream("%2$s");
                source.mapValues(decorator::decorate).to("%3$s");
                return source;
            }
        }
        """;

    private static final String DECORATOR = """
        package example;

        import jakarta.inject.Singleton;

        @Singleton
        public class Decorator {
            public String decorate(String value) {
                return "%s " + value;
            }
        }
        """;

    @TempDir
    Path project;

    @Test
    void anInPlaceChangeRebuildsTheStreamsAndARestartClosesThemAndLeavesTheRetiredGenerationCollectable() throws Exception {
        String bootstrap = Kafka.getProperties().get("kafka.bootstrap.servers");
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(new NewTopic(IN, 1, (short) 1), new NewTopic(OUT, 1, (short) 1))).all().get();
        } catch (Exception e) {
            // created by an earlier run against the same broker
        }
        try (ReloadHarness harness = ReloadHarness.inDirectory(project);
             KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap), new StringSerializer(), new StringSerializer());
             KafkaConsumer<String, String> output = new KafkaConsumer<>(Map.of(
                 ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                 ConsumerConfig.GROUP_ID_CONFIG, "dev-streams-test-" + UUID.randomUUID(),
                 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new StringDeserializer())) {
            output.subscribe(List.of(OUT));
            harness.property("kafka.bootstrap.servers", bootstrap);
            harness.property("kafka.health.enabled", "false");
            harness.property("kafka.streams." + STREAM + ".application-id", "dev-streams-" + UUID.randomUUID());
            harness.source("example.Topology", TOPOLOGY.formatted(STREAM, IN, OUT));
            harness.source("example.Decorator", DECORATOR.formatted("first"));
            harness.start();
            assertReloaderPresent(harness.context());

            KafkaStreams first = streams(harness.context());
            awaitRunning(first);
            producer.send(new ProducerRecord<>(IN, "one")).get();
            awaitOutput(output, "first one");

            // the factory that builds the topology changed in place: the streams are closed and built again
            long rebuildStart = System.nanoTime();
            changedInPlace(harness, "example.Topology");
            KafkaStreams rebuilt = streams(harness.context());
            assertNotSame(first, rebuilt);
            assertEquals(KafkaStreams.State.NOT_RUNNING, first.state());
            awaitRunning(rebuilt);
            producer.send(new ProducerRecord<>(IN, "two")).get();
            awaitOutput(output, "first two");
            // the closed streams left their group: the rebuilt ones get the partitions without waiting out the 45 second session timeout
            long rebuiltIn = Duration.ofNanos(System.nanoTime() - rebuildStart).toMillis();
            assertTrue(rebuiltIn < 30_000, "the rebuilt streams processed a record " + rebuiltIn + " ms after the change");
            System.out.println("The rebuilt streams processed a record " + rebuiltIn + " ms after the change");
            first = null;

            // a bean the factory delegates the topology to changed in place: the streams are built again with it
            KafkaStreams beforeDelegate = rebuilt;
            changedInPlace(harness, "example.Decorator");
            rebuilt = streams(harness.context());
            assertNotSame(beforeDelegate, rebuilt);
            assertEquals(KafkaStreams.State.NOT_RUNNING, beforeDelegate.state());
            beforeDelegate = null;
            awaitRunning(rebuilt);
            producer.send(new ProducerRecord<>(IN, "delegated")).get();
            awaitOutput(output, "first delegated");

            // the definition of the topology bean is swapped, as the development runtime does when it applies new definitions
            definitionsChanged(harness);
            KafkaStreams swapped = streams(harness.context());
            assertNotSame(rebuilt, swapped);
            assertEquals(KafkaStreams.State.NOT_RUNNING, rebuilt.state());
            awaitRunning(swapped);
            producer.send(new ProducerRecord<>(IN, "swapped")).get();
            awaitOutput(output, "first swapped");
            rebuilt = swapped;
            swapped = null;

            harness.source("example.Decorator", DECORATOR.formatted("second"));
            long restartStart = System.nanoTime();
            harness.reload();
            assertEquals(2, harness.generation());
            assertReloaderPresent(harness.context());
            // the retired context closed its streams as it stopped
            assertEquals(KafkaStreams.State.NOT_RUNNING, rebuilt.state());
            rebuilt = null;

            awaitRunning(streams(harness.context()));
            producer.send(new ProducerRecord<>(IN, "three")).get();
            awaitOutput(output, "second three");
            // the retired streams left their group as the restart began: the new ones do not wait out the session timeout
            long restartedIn = Duration.ofNanos(System.nanoTime() - restartStart).toMillis();
            assertTrue(restartedIn < 30_000, "the streams of the second generation processed a record " + restartedIn + " ms after the reload started");
            System.out.println("The streams of the second generation processed a record " + restartedIn + " ms after the reload started");

            // neither the streams of the first generation, their threads, nor the development-only reloaders keep it reachable
            ReloadTck.assertRetiredGenerationsCollected(harness);
        }
    }

    /**
     * Tells the running generation that a class was redefined in place, as the development runtime does after it
     * redefined the class. The context is not kept: a reference to it would keep the generation reachable.
     */
    private static void changedInPlace(ReloadHarness harness, String className) {
        ApplicationContext context = harness.context();
        context.publishEvent(new ClassChangeEvent(KafkaStreamsReloadTest.class, harness.generation(), Set.of(), context.getClassLoader(),
            List.of(new ClassChange(className, ClassChange.Kind.MODIFIED)), ReloadStrategy.RELOAD));
    }

    /**
     * Tells the running generation that the definition of the topology bean was retired and added again.
     */
    private static void definitionsChanged(ReloadHarness harness) {
        ApplicationContext context = harness.context();
        BeanDefinition<?> definition = context.getBeanDefinition(KStream.class, Qualifiers.byName(STREAM));
        ((DefaultBeanContext) context).notifyDefinitionChange(List.of(definition), List.of(definition));
    }

    private static KafkaStreams streams(ApplicationContext context) {
        return context.getBean(KafkaStreams.class, Qualifiers.byName(STREAM));
    }

    private static void awaitRunning(KafkaStreams streams) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (streams.state() != KafkaStreams.State.RUNNING) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("The streams are " + streams.state());
            }
            Thread.sleep(100);
        }
    }

    private static void awaitOutput(KafkaConsumer<String, String> output, String expected) {
        List<String> seen = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : output.poll(Duration.ofMillis(200))) {
                seen.add(record.value());
                if (record.value().equals(expected)) {
                    return;
                }
            }
        }
        throw new AssertionError("Expected " + expected + " but the topology produced " + seen);
    }

    private static void assertReloaderPresent(ApplicationContext context) {
        assertTrue(context.containsBean(type(context, "io.micronaut.configuration.kafka.streams.DevelopmentKafkaStreamsReloader")));
    }

    private static Class<?> type(ApplicationContext context, String className) {
        try {
            return Class.forName(className, true, context.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new AssertionError(className + " is not in the application", e);
        }
    }
}
