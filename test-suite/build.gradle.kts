plugins {
    id("io.micronaut.internal.build.kafka-testsuite")
}

dependencies {
    testAnnotationProcessor(platform(mn.micronaut.core.bom))
    testAnnotationProcessor(mn.micronaut.inject.java)
    testImplementation(mnTest.micronaut.test.junit5)
    testImplementation(mnTest.junit.platform.launcher)
    implementation(platform(mnTest.boms.testcontainers))
    implementation(libs.testcontainers.kafka)
}
