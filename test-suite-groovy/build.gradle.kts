plugins {
    groovy
    id("io.micronaut.internal.build.kafka-testsuite")
}

val testApplicationContextConfigurerMetadata = sourceSets.test.get().output.classesDirs.asFileTree.matching {
    include("META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer")
    include("META-INF/micronaut/io.micronaut.context.ApplicationContextConfigurer/**")
}

tasks.withType<Test>().configureEach {
    classpath = classpath.minus(testApplicationContextConfigurerMetadata)
}

dependencies {
    testImplementation(platform(mn.micronaut.core.bom))
    testCompileOnly(mn.micronaut.inject.groovy)
    testImplementation(mnTest.micronaut.test.spock)
    implementation(platform(mnTest.boms.testcontainers))
    implementation(libs.testcontainers.kafka)
    testImplementation(projects.testSuiteKafkaUtils)
}
