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
