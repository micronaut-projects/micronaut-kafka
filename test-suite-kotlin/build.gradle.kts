
plugins {
    id("io.micronaut.internal.build.kafka-testsuite")
    id("io.micronaut.build.internal.kotlin-kapt")
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
    kaptTest(platform(mn.micronaut.core.bom))
    kaptTest(mn.micronaut.inject.java)
    testImplementation(mnTest.micronaut.test.junit5)
    testImplementation(mn.kotlinx.coroutines.core)
    testImplementation(mnTest.junit.platform.launcher)
    testImplementation(platform(mnTest.boms.testcontainers))
    testImplementation(libs.testcontainers.kafka)
    testImplementation(projects.testSuiteKafkaUtils)
}
