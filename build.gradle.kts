
import org.gradle.api.file.SourceDirectorySet
import org.gradle.api.plugins.antlr.AntlrTask
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test
import org.gradle.external.javadoc.StandardJavadocDocletOptions
import org.gradle.testing.jacoco.tasks.JacocoCoverageVerification
import org.gradle.testing.jacoco.tasks.JacocoReport

plugins {
    `java-library`
    antlr
    jacoco
    `maven-publish`
    signing
    alias(libs.plugins.nexus)
}

repositories {
    mavenCentral()
}

val ossrhUsername = System.getenv("OSSRH_USERNAME")
val ossrhPassword = System.getenv("OSSRH_PASSWORD")

nexusPublishing {
    packageGroup.set(project.group.toString())
    repositories {
        sonatype {
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
            username.set(ossrhUsername)
            password.set(ossrhPassword)
        }
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
    withJavadocJar()
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(8)
}

tasks.withType<Javadoc>().configureEach {
    val options = options as StandardJavadocDocletOptions
    if (JavaVersion.current().isJava9Compatible) {
        options.addBooleanOption("html5", true)
    }
    options.encoding = "UTF-8"
    options.source("8")
    options.addStringOption("Xdoclint:none", "-quiet")
}

val testJvmArgs = listOf(
    "--enable-native-access=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.time=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
)

tasks.withType<Test>().configureEach {
    useJUnit()
    jvmArgs(testJvmArgs)
}

val sourceSets = the<SourceSetContainer>()
val mainAntlrSource = sourceSets.named("main").get().extensions.getByName("antlr") as SourceDirectorySet
mainAntlrSource.setSrcDirs(listOf("src/main/antlr4"))

dependencies {
    antlr(libs.antlr.tool)

    api(libs.concurrent.trees)
    implementation(libs.javassist)
    implementation(libs.sqlite.jdbc)
    implementation(libs.kryo)
    implementation(libs.kryo.serializers) {
        exclude(group = "com.esotericsoftware.kryo", module = "kryo")
    }
    implementation(libs.antlr.runtime)
    implementation(libs.typetools)

    testImplementation(libs.mockito.core) {
        exclude(group = "org.hamcrest", module = "hamcrest-core")
    }
    testImplementation(libs.junit4)
    testImplementation(libs.junit.dataprovider)
    testImplementation(libs.guava.testlib)
    testImplementation(libs.equalsverifier)
}

tasks.named<AntlrTask>("generateGrammarSource") {
    source = mainAntlrSource.asFileTree.matching {
        exclude("imports/**")
    }
    arguments.addAll(listOf("-lib", file("src/main/antlr4/imports").absolutePath))
}

val jacocoExcludes = listOf(
    "com/googlecode/cqengine/query/parser/*/grammar/*",
    "com/googlecode/cqengine/query/parser/cqn/support/ApacheSolrDataMathParser.*"
)

tasks.named<JacocoReport>("jacocoTestReport") {
    dependsOn(tasks.named("test"))
    classDirectories.setFrom(
        fileTree(layout.buildDirectory.dir("classes/java/main")) {
            exclude(jacocoExcludes)
        }
    )
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
    dependsOn(tasks.named("test"))
    classDirectories.setFrom(
        fileTree(layout.buildDirectory.dir("classes/java/main")) {
            exclude(jacocoExcludes)
        }
    )
}

tasks.named("check") {
    dependsOn(tasks.named("jacocoTestReport"))
    dependsOn(tasks.named("jacocoTestCoverageVerification"))
}

publishing {
    publications {
        create<MavenPublication>(project.name)
    }
}

apply(from = "publish.gradle")
