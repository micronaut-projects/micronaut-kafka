plugins {
    id("io.micronaut.internal.build.kafka-testsuite")
}

val applicationContextConfigurerMetadata = listOf(
    "META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer",
    "META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer/**"
)

val filteredTestRuntimeOutput = tasks.register<Sync>("filteredTestRuntimeOutput") {
    from(sourceSets.test.get().output)
    into(layout.buildDirectory.dir("filtered-test-runtime/test"))
    applicationContextConfigurerMetadata.forEach(::exclude)
}

tasks.withType<Test>().configureEach {
    dependsOn(filteredTestRuntimeOutput)
    classpath =
        files(filteredTestRuntimeOutput.map { it.destinationDir }) +
            (classpath - sourceSets.test.get().output)
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
