# CQEngine

[![Release](https://jitpack.io/v/boozilla/cqengine.svg)](https://jitpack.io/#boozilla/cqengine)

# Gradle Migration Guide

This project has been migrated from Maven to Gradle. This document provides information about the migration and how to use the new Gradle build.

## Prerequisites

- Java 11 or higher
- Gradle 8.5 or higher (optional, as the Gradle Wrapper is included)

## Getting Started

### Generating the Gradle Wrapper JAR

The Gradle Wrapper scripts (`gradlew` and `gradlew.bat`) are included, but the Gradle Wrapper JAR file needs to be generated. You can do this by running:

```bash
# If you have Gradle installed
gradle wrapper

# Or, if you don't have Gradle installed, you can download it from https://gradle.org/install/
```

### Building the Project

Once the Gradle Wrapper JAR is generated, you can build the project using:

```bash
# On Unix-like systems
./gradlew build

# On Windows
gradlew.bat build
```

### Running Tests

```bash
./gradlew test
```

### Creating JARs

- Regular JAR: `./gradlew jar`
- Fat JAR with all dependencies: `./gradlew shadowJar`
- Source JAR: `./gradlew sourcesJar`
- JavaDoc JAR: `./gradlew javadocJar`

## Migration Details

The Gradle build has been configured to match the Maven build as closely as possible:

1. **Java Version**: Java 8 compatibility is maintained.
2. **Dependencies**: All dependencies have been migrated with the same versions.
3. **Plugins**:
   - The ANTLR plugin is configured for grammar generation.
   - The Shadow plugin replaces the Maven Shade plugin for creating a fat JAR.
   - The BND plugin is used for OSGi metadata.
   - JaCoCo is configured for code coverage.
4. **Publishing**: The Maven publishing plugin is configured for publishing to Maven Central.

## Known Issues

- The package relocation in the Shadow JAR might not be exactly the same as in the Maven Shade plugin. If you encounter any issues, please report them.

## CI/CD Integration

If you're using CI/CD systems like Travis CI or GitHub Actions, you'll need to update your configuration to use Gradle instead of Maven.

Example for GitHub Actions:

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 11
        uses: actions/setup-java@v3
        with:
          java-version: '11'
          distribution: 'temurin'
      - name: Build with Gradle
        uses: gradle/gradle-build-action@v2
        with:
          arguments: build
```

## Additional Resources

- [Gradle User Manual](https://docs.gradle.org/current/userguide/userguide.html)
- [Gradle DSL Reference](https://docs.gradle.org/current/dsl/)
- [Shadow Plugin Documentation](https://imperceptiblethoughts.com/shadow/)
