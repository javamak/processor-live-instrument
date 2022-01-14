import java.io.FileOutputStream
import java.net.URL

plugins {
    id("com.avast.gradle.docker-compose")
    id("com.github.johnrengelman.shadow")
    kotlin("jvm")
    kotlin("kapt")
}

val processorGroup: String by project
val instrumentProcessorVersion: String by project
val processorDependenciesVersion: String by project
val skywalkingVersion: String by project
val vertxVersion: String by project
val grpcVersion: String by project
val protocolVersion: String by project
val jacksonVersion: String by project
val kotlinVersion: String by project
val joorVersion: String by project
val jupiterVersion: String by project

group = processorGroup
version = instrumentProcessorVersion

repositories {
    mavenCentral()
    maven(url = "https://jitpack.io")
}

dependencies {
    compileOnly("org.jooq:joor:$joorVersion")
    compileOnly("com.github.sourceplusplus:processor-dependencies:$processorDependenciesVersion")
    compileOnly("com.github.sourceplusplus.protocol:protocol:$protocolVersion")
    compileOnly("org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion")
    compileOnly("org.apache.skywalking:apm-network:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:library-server:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:library-module:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:telemetry-api:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:server-core:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:skywalking-sharing-server-plugin:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:library-client:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:skywalking-trace-receiver-plugin:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:agent-analyzer:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:event-analyzer:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:meter-analyzer:$skywalkingVersion") { isTransitive = false }
    compileOnly("org.apache.skywalking:log-analyzer:$skywalkingVersion") { isTransitive = false }
    compileOnly("io.vertx:vertx-service-discovery:$vertxVersion")
    compileOnly("io.vertx:vertx-service-proxy:$vertxVersion")
    compileOnly("io.vertx:vertx-codegen:$vertxVersion")
    compileOnly("org.jetbrains.kotlinx:kotlinx-datetime:0.3.2")
    kapt("io.vertx:vertx-codegen:$vertxVersion:processor")
    annotationProcessor("io.vertx:vertx-service-proxy:$vertxVersion")
    compileOnly("io.vertx:vertx-tcp-eventbus-bridge:$vertxVersion")
    compileOnly("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0")
    compileOnly("io.vertx:vertx-core:$vertxVersion")
    compileOnly("io.vertx:vertx-lang-kotlin:$vertxVersion")
    compileOnly("io.vertx:vertx-lang-kotlin-coroutines:$vertxVersion")
    compileOnly("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    compileOnly("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jacksonVersion")
    compileOnly("com.fasterxml.jackson.datatype:jackson-datatype-guava:$jacksonVersion")
    compileOnly("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    compileOnly("org.slf4j:slf4j-api:1.7.33")
    compileOnly("com.google.guava:guava:31.0.1-jre")
    compileOnly("io.grpc:grpc-stub:$grpcVersion") {
        exclude(mapOf("group" to "com.google.guava", "module" to "guava"))
    }
    compileOnly("io.grpc:grpc-netty:$grpcVersion") {
        exclude(mapOf("group" to "com.google.guava", "module" to "guava"))
    }
    compileOnly("io.grpc:grpc-protobuf:$grpcVersion") {
        exclude(mapOf("group" to "com.google.guava", "module" to "guava"))
    }

    testImplementation("io.vertx:vertx-core:$vertxVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$jupiterVersion")
    testImplementation("io.vertx:vertx-junit5:$vertxVersion")
    testImplementation("io.vertx:vertx-web-client:$vertxVersion")
    testImplementation("io.vertx:vertx-lang-kotlin-coroutines:$vertxVersion")
    testImplementation("com.github.sourceplusplus.protocol:protocol:$protocolVersion")
    testImplementation("io.vertx:vertx-tcp-eventbus-bridge:$vertxVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0")
    testImplementation(files(".ext/vertx-service-proxy-4.0.2.jar"))
    testImplementation("org.slf4j:slf4j-api:1.7.32")
    testImplementation("org.slf4j:slf4j-simple:1.7.33")
    testImplementation("com.google.guava:guava:31.0.1-jre")
    testImplementation("org.apache.skywalking:agent-analyzer:$skywalkingVersion")
    testImplementation("org.apache.skywalking:log-analyzer:$skywalkingVersion")
    testImplementation("io.vertx:vertx-lang-kotlin:$vertxVersion")
}

tasks.getByName<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("spp-processor-instrument")
    archiveClassifier.set("")

    exclude("DebugProbesKt.bin")
    exclude("lang-type-mapping.properties")
    exclude("META-INF/maven/**")
    exclude("META-INF/services/io.vertx.*")
    exclude("META-INF/versions/**")
    exclude("META-INF/vertx/vertx-service-proxy/**")
    exclude("META-INF/README.txt")
    exclude("META-INF/ABOUT.txt")
    exclude("META-INF/LICENSE.txt")
    exclude("META-INF/LICENSE")
    exclude("META-INF/NOTICE")
    exclude("META-INF/CHANGELOG")
    exclude("xsd/**")
    exclude("migrations/**")
    exclude("io/vertx/serviceproxy/**")

    minimize()

    dependencyFilter.exclude {
        it.moduleGroup == "org.jetbrains.kotlin"
                || it.moduleGroup == "org.jetbrains.kotlinx"
                || it.moduleGroup == "org.jetbrains"
                || it.moduleGroup == "org.intellij"
                || it.moduleGroup == "io.vertx"
                || it.moduleGroup == "io.netty"
                || it.moduleGroup == "org.slf4j"
                || it.moduleGroup == "io.r2dbc"
                || it.moduleGroup == "com.google.errorprone"
                || it.moduleGroup == "com.google.guava"
                || it.moduleGroup == "com.google.code.findbugs"
                || it.moduleGroup.startsWith("com.fasterxml.")
                || it.moduleGroup.contains(".sourceplusplus.protocol")
    }
}
tasks.getByName("jar").dependsOn("shadowJar")

tasks {
    withType<JavaCompile> {
        sourceCompatibility = "1.8"
        targetCompatibility = "1.8"
    }

    register("downloadProbe") {
        doLast {
            val f = File(projectDir, "e2e/spp-probe-0.2.1.jar")
            if (!f.exists()) {
                println("Downloading Source++ JVM probe")
                URL("https://github.com/sourceplusplus/probe-jvm/releases/download/0.2.1/spp-probe-0.2.1.jar")
                    .openStream().use { input ->
                        FileOutputStream(f).use { output ->
                            input.copyTo(output)
                        }
                    }
                println("Downloaded Source++ JVM probe")
            }
        }
    }
    register("downloadProbeServices") {
        doLast {
            val f = File(projectDir, "e2e/spp-skywalking-services-0.2.1.jar")
            if (!f.exists()) {
                println("Downloading Source++ JVM probe services")
                URL("https://github.com/sourceplusplus/probe-jvm/releases/download/0.2.1/spp-skywalking-services-0.2.1.jar")
                    .openStream().use { input ->
                        FileOutputStream(f).use { output ->
                            input.copyTo(output)
                        }
                    }
                println("Downloaded Source++ JVM probe services")
            }
        }
    }
    register("downloadProcessorDependencies") {
        doLast {
            val f = File(projectDir, "e2e/spp-processor-dependencies-$processorDependenciesVersion.jar")
            if (!f.exists()) {
                println("Downloading Source++ processor dependencies")
                URL("https://github.com/sourceplusplus/processor-dependencies/releases/download/$processorDependenciesVersion/spp-processor-dependencies-$processorDependenciesVersion.jar")
                    .openStream().use { input ->
                        FileOutputStream(f).use { output ->
                            input.copyTo(output)
                        }
                    }
                println("Downloaded Source++ processor dependencies")
            }
        }
    }
    register<Copy>("updateDockerFiles") {
        dependsOn("shadowJar")

        from("build/libs/spp-processor-instrument-$version.jar")
        into(File(projectDir, "e2e"))
    }

    register("assembleUp") {
        dependsOn(
            "downloadProbe", "downloadProbeServices", "downloadProcessorDependencies",
            "shadowJar", "updateDockerFiles", "composeUp"
        )
    }
    getByName("composeUp").mustRunAfter(
        "downloadProbe", "downloadProbeServices", "downloadProcessorDependencies",
        "shadowJar", "updateDockerFiles"
    )
}
tasks.getByName<Test>("test") {
    failFast = true
    useJUnitPlatform()
    if (System.getProperty("test.profile") != "integration") {
        exclude("integration/**")
    }

    testLogging {
        events("passed", "skipped", "failed")
        setExceptionFormat("full")

        outputs.upToDateWhen { false }
        showStandardStreams = true
    }
}

dockerCompose {
    dockerComposeWorkingDirectory.set(File("./e2e"))
    removeVolumes.set(true)
    waitForTcpPorts.set(false)
}
