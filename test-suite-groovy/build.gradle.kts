plugins {
    groovy
    id("io.micronaut.internal.build.kafka-testsuite")
}

dependencies {
    testImplementation(platform(mn.micronaut.core.bom))
    testCompileOnly(mn.micronaut.inject.groovy)
    testImplementation(mnTest.micronaut.test.spock)
    implementation(platform(mnTest.boms.testcontainers))
    implementation(libs.testcontainers.kafka)
    testImplementation(projects.testSuiteKafkaUtils)
}

//TODO remove once Micronaut Test ships Spock version compatible with Groovy 5
configurations.all {
    resolutionStrategy {
        force("org.spockframework:spock-core:2.4-M7-groovy-5.0")
    }
}
