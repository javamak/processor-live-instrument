# ![](https://github.com/sourceplusplus/live-platform/blob/master/.github/media/sourcepp_logo.svg)

[![License](https://img.shields.io/github/license/sourceplusplus/processor-live-instrument)](LICENSE)
![GitHub release](https://img.shields.io/github/v/release/sourceplusplus/processor-live-instrument?include_prereleases)
[![Build](https://github.com/sourceplusplus/processor-live-instrument/actions/workflows/build.yml/badge.svg)](https://github.com/sourceplusplus/processor-live-instrument/actions/workflows/build.yml)

## What is this?

This project provides additional backend processing to the [Source++](https://github.com/sourceplusplus/live-platform) open-source live coding platform.

## Features

Introduces **Live Instruments**, ephemeral production debugging devices for live applications. Live Instruments come in a number of types described below.

### Live Breakpoint

> Live Breakpoints (a.k.a non-breaking breakpoints) are useful debugging instruments for gaining insight into the live variables available in production at a given scope.

<details>
  <summary>Screencast</summary>

  ![live-breakpoint](https://user-images.githubusercontent.com/3278877/136304451-2c98ad30-032b-4ce0-9f37-f98cd750adb3.gif)
</details>

### Live Log

> Live Logs (a.k.a just-in-time logging) are quick and easy debugging instruments for instantly outputting live data from production without redeploying or restarting your application.

<details>
  <summary>Screencast</summary>

  ![live-log](https://user-images.githubusercontent.com/3278877/136304738-d46c2796-4dd3-45a3-81bb-5692547c1c71.gif)  
</details>

## Requirements

- SkyWalking OAP
  - Version >= 8.0.0
- Modules
  - `CoreModule`
  - `AnalyzerModule`
  - `StorageModule`
  - `LogAnalyzerModule`

## Development

### Prerequisites

- Git
- JDK 11+
- Docker
- Docker Compose

### Build

```sh
git clone https://github.com/sourceplusplus/processor-live-instrument
cd processor-live-instrument
./gradlew build
```

### Test

```sh
./gradlew assembleUp test -Dtest.profile=integration
```
