plugins {
    id("io.micronaut.internal.build.kafka-testsuite")
    id("io.micronaut.build.internal.python")
}

// Like the Java suite, keep the RuntimeBootstrapServers documentation snippet (a @ContextConfigurer pointing at
// localhost:9092) off the test runtime classpath; the KafkaTestConfigurer of src/test/java stays registered.
val runtimeBootstrapServersConfigurerMetadata =
    "META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer/*.RuntimeBootstrapServers"

val filteredTestRuntimeOutput = tasks.register<Sync>("filteredTestRuntimeOutput") {
    from(sourceSets.test.get().output)
    into(layout.buildDirectory.dir("filtered-test-runtime/test"))
    exclude(runtimeBootstrapServersConfigurerMetadata)
}

tasks.withType<Test>().configureEach {
    dependsOn(filteredTestRuntimeOutput)
    classpath =
        files(filteredTestRuntimeOutput.map { it.destinationDir }) +
            (classpath - sourceSets.test.get().output)
    systemProperty("micronaut.python.pool.enabled", "false")
}

dependencies {
    // The Java KafkaTestConfigurer helper (src/test/java) is processed by javac
    testAnnotationProcessor(platform(mn.micronaut.core.bom))
    testAnnotationProcessor(mn.micronaut.inject.java)
    // Annotation processors of the Python sources MUST be testImplementation (not testAnnotationProcessor):
    // the Python compiler takes the compile classpath as its annotation processor path.
    testImplementation(mn.micronaut.inject.python.test)
    testImplementation(mn.micronaut.context.python)
    testImplementation(mnSerde.micronaut.serde.processor)
    testImplementation(mnTest.micronaut.test.junit5)
    testImplementation(mnTest.junit.platform.launcher)
    implementation(platform(mnTest.boms.testcontainers))
    implementation(libs.testcontainers.kafka)
    testImplementation(projects.testSuiteKafkaUtils)
}
