plugins {
    java
}

dependencies {
    testAnnotationProcessor(platform(mn.micronaut.core.bom))
    testAnnotationProcessor(mn.micronaut.inject.java)
    testImplementation(mnSerde.micronaut.serde.jackson)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(mnTest.micronaut.test.junit5)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.awaitility)
    testImplementation(projects.micronautKafka)
}

tasks.withType<Test> {
    useJUnitPlatform()
}
