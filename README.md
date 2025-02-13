# Velox4j: Java Bindings for Velox

## Project Status

Velox4j is currently a **concept project**.

## Introduction

### What is Velox?

Velox is an "opensource unified execution engine" as it states. The project was originally
funded by Meta in 2020. Projects often use Velox as a C++ library to accelerate SQL query executions.


Homepages of Velox:

- [GitHub repository](https://github.com/facebookincubator/velox)
- [Official website](https://velox-lib.io/)

Critical open source projects depending on Velox:

- [Presto](https://github.com/prestodb/presto)
- [Apache Gluten (incubating)](https://github.com/apache/incubator-gluten)


### What is Velox4j?

Velox4j is the Java bindings for Velox. It enables JVM applications to directly invoke Velox's
functionalities without writing and maintaining any C++ / JNI code.


## Platform Prerequisites

The project is not only tested on the following CPU architectures:

- x86-64

and on the following operating systems:

- Linux

Supports for platforms not on the above list will not be guaranteed by the main stream code of
Velox4j at the time. But certainly, contributions are always welcomed if anyone tends to involve.


## Releases

### Maven

```
<dependency>
  <groupId>io.github.zhztheplayer</groupId>
  <artifactId>velox4j</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```


## Get started

The following is a brief example of using Velox4j to execute a query:

```
TODO
```

## Design

### Portable

Velox4j is designed to be portable. The eventual goal is to make one Velox4j release to be
shipped onto difference platforms without rebuilding the Jar file.

### Seamless Velox API Mapping

Velox4j directly adopts Velox's existing JSON serde framework and implements the following
JSON-serializable Velox components in Java-side:

- Data types
- Query plans
- Expressions

With the help of Velox's own JSON serde, there will be no re-interpreting layer for query plans
in Velox4j's C++ code base. Which means, the Java side Velox components defined in Velox4j's
Java code will be 1-on-1 mapped to Velox's associated components. The design makes Velox4j's
code base even small, and any new Velox features easy to add to Velox4j.

### Compatible with Arrow

Velox4j is compatible with Apache Arrow. The output of Velox4j query can be exported into Arrow
format through utility APIs provided by Velox4j.

## License

This project is licensed under the [Apache-2.0 License](LICENSE).