# Velox4j: Java Bindings for Velox

## Project Status

Velox4j is currently a **concept project**.

## Introduction

### What is Velox?

Velox is an "open source unified execution engine" as it states. The project was originally
funded by Meta in 2020. Projects often use Velox as a C++ library to accelerate SQL query
executions.

Homepages of Velox:

- [GitHub repository](https://github.com/facebookincubator/velox)
- [Official website](https://velox-lib.io/)

Critical open source projects depending on Velox:

- [Presto](https://github.com/prestodb/presto)
- [Apache Gluten (incubating)](https://github.com/apache/incubator-gluten)

### What is Velox4j?

Velox4j is the Java bindings for Velox. It enables JVM applications to directly invoke Velox's
functionalities without writing and maintaining any C++ / JNI code.

## Design

Velox4j is designed within the following manners:

### Portable

Velox4j is designed to be portable. The eventual goal is to make one Velox4j release to be
shipped onto difference platforms without rebuilding the Jar file.

### Seamless Velox API Mapping

Velox4j directly adopts Velox's existing JSON serde framework and implements the following
JSON-serializable Velox components in Java-side:

- Data types
- Query plans
- Expressions
- Connectors

With the help of Velox's own JSON serde, there will be no re-interpreting layer for query plans
in Velox4j's C++ code base. Which means, the Java side Velox components defined in Velox4j's
Java code will be 1-on-1 mapped to Velox's associated components. The design makes Velox4j's
code base even small, and any new Velox features easy to add to Velox4j.

### Compatible with Arrow

Velox4j is compatible with Apache Arrow. The output of Velox4j query can be exported into Arrow
format through utility APIs provided by Velox4j.

## Prerequisites

### Platform 

The project is now only tested on the following CPU architectures:

- x86-64

and on the following operating systems:

- Linux

Supports for platforms not on the above list will not be guaranteed to have by the main stream code
of Velox4j at the time. But certainly, contributions are always welcomed if anyone tends to involve.

### Build Toolchains

The minimum toolchain versions for building Velox4j:

- GCC 11
- JDK 11

## Releases

Velox4j currently only provides SNAPSHOT jar releases.

### Maven

```xml
<dependency>
  <groupId>io.github.zhztheplayer</groupId>
  <artifactId>velox4j</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

If the `Central Portal Snapshots` repository is not set in Maven, the following settings also need
to be added:

```xml
<repositories>
  <repository>
    <name>Central Portal Snapshots</name>
    <id>central-portal-snapshots</id>
    <url>https://central.sonatype.com/repository/maven-snapshots/</url>
    <releases>
      <enabled>false</enabled>
    </releases>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>
```

NOTE: The released Jar is built with x86-64 CPU + CentOS 7 operating system. Which means
it may not work (say, crash when you use it) as expected on different platforms, especially on
Linux distributions with lower versions of glibc.

## Build From Source

```bash
mvn clean install -DskipTests
```

## Get Started

The following is a brief example of using Velox4j to execute a query:

```java
// 1. Initialize Velox4j.
Velox4j.ensureInitialized();

// 2. Define the plan output schema.
final RowType outputType = new RowType(List.of(
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment"
    ), List.of(
        new BigIntType(),
        new VarCharType(),
        new BigIntType(),
        new VarCharType()
    ));

// 3. Create a table scan node.
final TableScanNode scanNode = new TableScanNode(
    "plan-id-1",
    outputType,
    new HiveTableHandle(
        "connector-hive",
        "table-1",
        false,
        List.of(),
        null,
        outputType,
        Map.of()
    ),
    toAssignments(outputType)
);

// 4. Create a split associating with the table scan node, this makes
// the scan read a local file "/tmp/nation.parquet".
final File file = new File("/tmp/nation.parquet");
final BoundSplit split = new BoundSplit(
    scanNode.getId(),
    -1,
    new HiveConnectorSplit(
        "connector-hive",
        0,
        false,
        file.getAbsolutePath(),
        FileFormat.PARQUET,
        0,
        file.length(),
        Map.of(),
        OptionalInt.empty(),
        Optional.empty(),
        Map.of(),
        Optional.empty(),
        Map.of(),
        Map.of(),
        Optional.empty(),
        Optional.empty()
    )
);

// 5. Build the query.
final Query query = new Query(scanNode, List.of(split), Config.empty(), ConnectorConfig.empty());

// 6. Create a JNI session.
final MemoryManager memoryManager = MemoryManager.create(AllocationListener.NOOP);
final JniApi jniApi = JniApi.create(memoryManager);

// 7. Execute the query.
final UpIterator itr = query.execute(jniApi);

// 8. Collect and print results.
while (itr.hasNext()) {
  final RowVector rowVector = itr.next(); // 8.1. Get next RowVector returned by Velox.
  final VectorSchemaRoot vsr = Arrow.toArrowTable(new RootAllocator(), rowVector).toVectorSchemaRoot(); // 8.2. Convert the RowVector into Arrow format (an Arrow VectorSchemaRoot in this case).
  System.out.println(vsr.contentToTSVString()); // 8.3. Print the arrow table to stdout.
  vsr.close(); // 8.4. Release the Arrow VectorSchemaRoot.
}

// 9. Close the JNI session.
jniApi.close();
memoryManager.close();
```

Code of the `toAssignment` utility method used above:

```java
private static List<Assignment> toAssignments(RowType rowType) {
  final List<Assignment> list = new ArrayList<>();
  for (int i = 0; i < rowType.size(); i++) {
    final String name = rowType.getNames().get(i);
    final Type type = rowType.getChildren().get(i);
    list.add(new Assignment(name,
        new HiveColumnHandle(name, ColumnType.REGULAR, type, type, List.of())));
  }
  return list;
}
```

## License

This project is licensed under the [Apache-2.0 License](LICENSE).
