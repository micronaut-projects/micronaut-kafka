
plugins {
    id("io.micronaut.internal.build.kafka-testsuite")
    id("io.micronaut.build.internal.kotlin-kapt")
}

val filteredTestClassesDir = layout.buildDirectory.dir("filtered-test-classes")

val filteredTestClasses by tasks.registering(org.gradle.api.tasks.Sync::class) {
    from(sourceSets.test.get().output.classesDirs)
    into(filteredTestClassesDir)
    includeEmptyDirs = false
    exclude("META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer")
    exclude("META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer/**")
}

tasks.withType<Test>().configureEach {
    dependsOn(filteredTestClasses)
    testClassesDirs = files(filteredTestClassesDir)
    classpath = files(filteredTestClassesDir) + (classpath - sourceSets.test.get().output.classesDirs)
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
