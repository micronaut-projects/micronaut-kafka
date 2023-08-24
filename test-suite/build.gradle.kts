plugins {
    java
    id("io.micronaut.internal.build.kafka-testsuite")
}

dependencies {
    testAnnotationProcessor(platform(mn.micronaut.core.bom))
    testAnnotationProcessor(mn.micronaut.inject.java)
    testImplementation(mn.micronaut.messaging)
    testImplementation(mnSerde.micronaut.serde.jackson)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(mnTest.micronaut.test.junit5)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(mnLogging.logback.classic)
    testImplementation(libs.awaitility)
    testImplementation(mnReactor.micronaut.reactor)
    testImplementation(mnSerde.micronaut.serde.jackson)
    testImplementation(mnRxjava2.micronaut.rxjava2)
    testImplementation(projects.micronautKafka)
    testImplementation(projects.micronautKafkaStreams)
}
