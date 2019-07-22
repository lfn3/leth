import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.0"
}

group = "net.lfn3"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jooq:jooq:3.11.11")

    testImplementation("com.h2database:h2:1.4.199")
    testImplementation("org.jooq:jooq-meta:3.11.11")
    testImplementation("org.jooq:jooq-codegen:3.11.11")
    testImplementation(kotlin("stdlib-jdk8"))
    testImplementation(kotlin("test-junit5"))
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}