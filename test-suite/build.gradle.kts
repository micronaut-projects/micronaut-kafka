plugins {
    java
    id("io.micronaut.internal.build.kafka-testsuite")
}

dependencies {
    testAnnotationProcessor(platform(mn.micronaut.core.bom))
    testAnnotationProcessor(mn.micronaut.inject.java)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(mnTest.micronaut.test.junit5)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.awaitility)
    testImplementation (mnSerde.micronaut.serde.jackson)
    testImplementation(projects.micronautKafka)
}
