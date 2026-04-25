plugins {
    id("io.micronaut.internal.build.kafka-testsuite")
}

tasks.withType<Test>().configureEach {
    doFirst {
        delete(
            sourceSets.test.get().output.classesDirs.asFileTree.matching {
                include("META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer")
                include("META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer/**")
            }
        )
    }
}

dependencies {
    testAnnotationProcessor(platform(mn.micronaut.core.bom))
    testAnnotationProcessor(mn.micronaut.inject.java)
    testImplementation(mnTest.micronaut.test.junit5)
    testImplementation(mnTest.junit.platform.launcher)
    implementation(platform(mnTest.boms.testcontainers))
    implementation(libs.testcontainers.kafka)
    testImplementation(projects.testSuiteKafkaUtils)
}
