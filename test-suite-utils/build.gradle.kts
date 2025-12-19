plugins {
    id("io.micronaut.build.internal.java-base")
}
dependencies {
    implementation(platform(mnTest.boms.testcontainers))
    implementation(libs.testcontainers.kafka)
}
