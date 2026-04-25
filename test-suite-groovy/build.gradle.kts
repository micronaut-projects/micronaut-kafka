plugins {
    groovy
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
    testImplementation(platform(mn.micronaut.core.bom))
    testCompileOnly(mn.micronaut.inject.groovy)
    testImplementation(mnTest.micronaut.test.spock)
    implementation(platform(mnTest.boms.testcontainers))
    implementation(libs.testcontainers.kafka)
    testImplementation(projects.testSuiteKafkaUtils)
}
