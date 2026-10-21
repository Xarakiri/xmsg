// SPDX-License-Identifier: AGPL-3.0-only
plugins {
    kotlin("jvm") version "2.2.0"
    id("com.google.protobuf") version "0.9.5"
    application
}

group = "dev.xmsg"
version = "0.0.0"

repositories { mavenCentral() }

val kafkaVersion = "4.0.0"
val protobufVersion = "4.29.3"

dependencies {
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("org.slf4j:slf4j-simple:2.0.16")

    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin { jvmToolchain(21) }

// The contracts live in the repository-level proto/ directory (Buf module).
sourceSets { main { proto { srcDir("../../proto") } } }

protobuf { protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" } }

application { mainClass.set("dev.xmsg.sequencer.AppKt") }

tasks.test {
    useJUnitPlatform()
    systemProperty("xmsg.vectors", file("../../spec/partitioning/vectors.csv").absolutePath)
    testLogging { events("passed", "failed", "skipped") }
}
