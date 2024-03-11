plugins {
    id("io.micronaut.internal.build.kafka-testsuite")
    id("org.jetbrains.kotlin.jvm") version mn.versions.kotlin
    id("org.jetbrains.kotlin.kapt") version mn.versions.kotlin
}

dependencies {
    kaptTest(platform(mn.micronaut.core.bom))
    kaptTest(mn.micronaut.inject.java)
    testImplementation(mnTest.micronaut.test.junit5)
    testImplementation(mn.kotlinx.coroutines.core)
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}
