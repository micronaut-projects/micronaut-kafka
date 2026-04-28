
plugins {
    id("io.micronaut.internal.build.kafka-testsuite")
    id("io.micronaut.build.internal.kotlin-kapt")
}

val applicationContextConfigurerMetadata = listOf(
    "META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer",
    "META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer/**"
)
val kaptTestClassesDir = layout.buildDirectory.dir("tmp/kapt3/classes/test")
val filteredTestRuntimeDir = layout.buildDirectory.dir("filtered-test-runtime/test")

val filteredTestRuntimeOutput by tasks.registering(Sync::class) {
    from(sourceSets.test.get().output)
    from(kaptTestClassesDir)
    into(filteredTestRuntimeDir)
    includeEmptyDirs = false
    applicationContextConfigurerMetadata.forEach(::exclude)
}

tasks.withType<Test>().configureEach {
    dependsOn(filteredTestRuntimeOutput)
    testClassesDirs = files(filteredTestRuntimeDir)
    classpath =
        files(filteredTestRuntimeDir) +
            (classpath - sourceSets.test.get().output - files(kaptTestClassesDir))
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
