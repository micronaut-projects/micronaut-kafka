plugins {
    id("org.jetbrains.kotlin.jvm") version "1.9.0"
    id("org.jetbrains.kotlin.kapt") version "1.9.0"
    id("io.micronaut.internal.build.kafka-testsuite")
}

dependencies {
    kaptTest(platform(mn.micronaut.core.bom))
    kaptTest(mn.micronaut.inject.java)
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
}
