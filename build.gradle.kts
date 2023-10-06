plugins {
    kotlin("jvm") version "1.9.0"
    antlr
    id("com.avast.gradle.docker-compose") version "0.17.5"
    id("java-test-fixtures")
}

group = "com.cjuega"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

sourceSets {
    main {
        java {
            srcDirs("src/main/generated")
        }
    }

    create("intTest") {
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

val intTestImplementation by configurations.getting {
    extendsFrom(configurations.testImplementation.get())
}
val intTestRuntimeOnly by configurations.getting

configurations["intTestRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())

val javaComponent = components["java"] as AdhocComponentWithVariants
javaComponent.withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
javaComponent.withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }

dependencies {
    antlr("org.antlr:antlr4:4.9.2")
    compileOnly("org.antlr:antlr4-runtime:4.9.2")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation(platform("software.amazon.awssdk:bom:2.20.157"))
    implementation("software.amazon.awssdk:dynamodb")

    testImplementation(kotlin("test"))
    testImplementation("org.assertj:assertj-core:3.6.1")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.0")

    testFixturesImplementation("net.datafaker:datafaker:2.0.2")
}

val integrationTest = task<Test>("intTest") {
    description = "Runs integration tests."
    group = "verification"

    testClassesDirs = sourceSets["intTest"].output.classesDirs
    classpath = sourceSets["intTest"].runtimeClasspath
    shouldRunAfter("test")

    useJUnitPlatform()

    testLogging {
        events("passed")
    }
}

tasks {
    clean {
        delete("src/main/generated")
    }

    build {
        dependsOn(clean)
    }

    test {
        useJUnitPlatform()
    }

    check {
        dependsOn(integrationTest)
    }

    dockerCompose {
        checkContainersRunning = true

        stopContainers = true
        removeContainers = true
        removeOrphans = true

        waitForTcpPorts = true

        isRequiredBy(integrationTest)
    }

    generateGrammarSource {
        arguments = arguments + listOf("-visitor", "-no-listener")
        outputDirectory = file("$projectDir/src/main/generated/com/cjuega/easydynamodb/expressions")
    }

    compileKotlin {
        dependsOn(generateGrammarSource)
    }

    compileTestKotlin {
        dependsOn(generateTestGrammarSource)
    }

    compileTestFixturesKotlin {
        dependsOn(generateTestFixturesGrammarSource)
    }
}

kotlin {
    jvmToolchain(20)
}
