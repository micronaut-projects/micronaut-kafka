plugins {
    groovy
    id("io.micronaut.internal.build.kafka-testsuite")
}

dependencies {
    testImplementation(platform(mn.micronaut.core.bom))
    testCompileOnly(mn.micronaut.inject.groovy)
    testImplementation(mnSerde.micronaut.serde.jackson)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(mnTest.micronaut.test.spock)
    testRuntimeOnly(mnLogging.logback.classic)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(mnReactor.micronaut.reactor)
    testImplementation(mnSerde.micronaut.serde.jackson)
    testImplementation(mnRxjava2.micronaut.rxjava2)
    testImplementation(projects.micronautKafka)
    testImplementation(projects.micronautKafkaStreams)
}
