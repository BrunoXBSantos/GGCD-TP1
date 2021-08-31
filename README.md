[bruno]: https://github.com/BrunoXBSantos
[bruno-pic]: https://github.com/BrunoXBSantos.png?size=120
[nelson]: https://github.com/nelsonmestevao
[nelson-pic]: https://github.com/nelsonmestevao.png?size=120
[rui]: https://github.com/Syrayse
[rui-pic]: https://github.com/Syrayse.png?size=120
[susana]: https://github.com/SusanaMarques
[susana-pic]: https://github.com/SusanaMarques.png?size=120

<div align="center">

# Data processing and storage

[Geeting Started](#rocket-getting-started)
|
[Development](#hammer-development)
|
[Tools](#hammer_and_wrench-tools)
|
[Team](#busts_in_silhouette-team)

</div>

The practical work consists of carrying out the experimental evaluation of data
storage and processing tasks using Hadoop HDFS, Avro + Parquet and MapReduce
using [IMDb public datasets](https://www.imdb.com/interfaces/).

## :rocket: Getting Started

These instructions will get you a copy of the project up and running on your
local machine for development and testing purposes.

Start by filling out the environment variables defined in the `.env` file. Use
the `.env.sample` as a starting point.

```bash
bin/setup
```

After this, you must fill in the fields correctly and export them in your
environment. Checkout [direnv](https://direnv.net/) for your shell and
[EnvFile](https://github.com/Ashald/EnvFile) for IntelliJ.

### :inbox_tray: Prerequisites

The following software is required to be installed on your system:

- [Java SDK 8+](https://openjdk.java.net/)
- [Maven](https://maven.apache.org/maven-features.html)
- [GCloud CLI](https://cloud.google.com/sdk/docs/install)

### :hammer: Development

Run the project.

```
bin/run (--local | --gcloud) <args>
```

Run an interactive Java shell for quick testing.

```
bin/jshell
```

Build the project.

```
bin/build
```

Run the tests.

```
bin/test
```

Format the code accordingly to common [guide lines](https://github.com/google/google-java-format).

```
bin/format
```

Lint your code with _checkstyle_.

```
bin/lint
```

Generate the documentation.

```
bin/docs
```

Clean the repository.

```
bin/clean
```

### :hammer_and_wrench: Tools

The recommended Integrated Development Environment (IDE) is IntelliJ IDEA.

## :busts_in_silhouette: Team

| [![Bruno][bruno-pic]][bruno] | [![Nelson][nelson-pic]][nelson] | [![Rui][rui-pic]][rui] | [![Susana][susana-pic]][susana] |
| :--------------------------: | :-----------------------------: | :--------------------: | :-----------------------------: |
|    [Bruno Santos][bruno]     |    [Nelson Estevão][nelson]     |    [Rui Reis][rui]     |    [Susana Marques][susana]     |
