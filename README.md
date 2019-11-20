# Micronaut Kafka

[![Maven Central](https://img.shields.io/maven-central/v/io.micronaut.configuration/micronaut-kafka.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.micronaut.configuration%22%20AND%20a:%22micronaut-kafka%22)
[![](https://github.com/micronaut-projects/micronaut-kafka/workflows/Java%20CI/badge.svg)](https://github.com/micronaut-projects/micronaut-kafka/actions)

This project includes integration between [Micronaut](http://micronaut.io) and [Kafka](https://kafka.apache.org).

## Documentation

See the [Documentation](https://micronaut-projects.github.io/micronaut-kafka/latest/guide) for more information.

## Snapshots and Releases

Snaphots are automatically published to JFrog OSS using [Github Actions](https://github.com/micronaut-projects/micronaut-kafka/actions).

See the documentation in the [Micronaut Docs](https://docs.micronaut.io/latest/guide/index.html#usingsnapshots) for how to configure your build to use snapshots.

Releases are published to JCenter and Maven Central via [Github Actions](https://github.com/micronaut-projects/micronaut-kafka/actions).

A release is performed with the following steps:

* [Edit the version](https://github.com/micronaut-projects/micronaut-kafka/edit/master/gradle.properties) specified by `projectVersion` in `gradle.properties` to a semantic, unreleased version. Example `1.0.0`
* [Create a new release](https://github.com/micronaut-projects/micronaut-kafka/releases/new). The Git Tag should start with `v`. For example `v1.0.0`.
* [Monitor the Workflow](https://github.com/micronaut-projects/micronaut-kafka/actions?query=workflow%3ARelease) to check it passed successfully.
* Celebrate!
