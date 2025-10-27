plugins {
    id("io.micronaut.internal.build.kafka-testsuite")
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.kapt)
}

dependencies {
    kaptTest(platform(mn.micronaut.core.bom))
    kaptTest(mn.micronaut.inject.java)
    testImplementation(mnTest.micronaut.test.junit5)
    testImplementation(mn.kotlinx.coroutines.core)
    testImplementation(mnTest.junit.platform.launcher)
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}
