// SPDX-License-Identifier: AGPL-3.0-only
import java.time.Duration
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.4.21"
    id("com.google.protobuf") version "0.10.0"
    application
    `jvm-test-suite`
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

// Built and run on JDK 27. Kotlin 2.4 emits bytecode up to Java 26, so both compilers
// target 26 (runs unchanged on 27); raise it when Kotlin supports 27.
kotlin {
    jvmToolchain(27)
    compilerOptions { jvmTarget.set(JvmTarget.JVM_26) }
}
tasks.withType<JavaCompile>().configureEach { options.release.set(26) }

// The contracts live in the repository-level proto/ directory (Buf module).
sourceSets { main { proto { srcDir("../../proto") } } }

protobuf { protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" } }

application { mainClass.set("dev.xmsg.sequencer.AppKt") }

tasks.test {
    useJUnitPlatform()
    systemProperty("xmsg.vectors", file("../../spec/partitioning/vectors.csv").absolutePath)
    testLogging { events("passed", "failed", "skipped") }
}

// Integration tests against a real Kafka broker (Testcontainers; needs Docker).
// Run with ./gradlew integrationTest. Not part of ./gradlew test or check.
testing {
    suites {
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter("5.11.4")
            dependencies {
                implementation(project())
                implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
                implementation("com.google.protobuf:protobuf-java:$protobufVersion")
                implementation("org.testcontainers:kafka:1.21.0")
                implementation("org.jetbrains.kotlin:kotlin-test")
                runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
            }
            targets {
                all {
                    testTask.configure {
                        shouldRunAfter(tasks.test)
                        // Crash test intensity: number of forced kills (CI default; raise for soak runs).
                        systemProperty("xmsg.crash.kills", System.getProperty("xmsg.crash.kills") ?: "12")
                        systemProperty("xmsg.it.logs", layout.buildDirectory.dir("it-logs").get().asFile.absolutePath)
                        testLogging {
                            events("passed", "failed", "skipped")
                            showStandardStreams = false
                            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
                        }
                        timeout.set(Duration.ofMinutes(20))
                    }
                }
            }
        }
    }
}
