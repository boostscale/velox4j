/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.github.zhztheplayer.velox4j.serde;

import java.io.*;
import java.util.Collections;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.ComparisonFailure;

import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.connector.*;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.filter.AlwaysTrue;
import io.github.zhztheplayer.velox4j.jni.JniApiTests;
import io.github.zhztheplayer.velox4j.jni.LocalSession;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.plan.WindowNode;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;
import io.github.zhztheplayer.velox4j.serializable.ISerializableCo;
import io.github.zhztheplayer.velox4j.sort.SortOrder;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.MapType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.Variant;
import io.github.zhztheplayer.velox4j.variant.VariantCo;
import io.github.zhztheplayer.velox4j.window.BoundType;
import io.github.zhztheplayer.velox4j.window.WindowFrame;
import io.github.zhztheplayer.velox4j.window.WindowFunction;
import io.github.zhztheplayer.velox4j.window.WindowType;

public final class SerdeTests {
  private static void assertJsonEquals(String expected, String actual) {
    final JsonNode expectedTree = Serde.parseTree(expected);
    final JsonNode actualTree = Serde.parseTree(actual);
    if (!actualTree.equals(expectedTree)) {
      throw new ComparisonFailure("", expected, actual);
    }
  }

  public static <T extends ISerializable> ObjectAndJson<ISerializable> testISerializableRoundTrip(
      T inObj) {
    try (final MemoryManager memoryManager = MemoryManager.create(AllocationListener.NOOP);
        final LocalSession session = JniApiTests.createLocalSession(memoryManager)) {
      final String inJson = Serde.toPrettyJson(inObj);

      {
        final byte[] serialized = serialize(inObj);
        final ISerializable javaDeserialized = deserialize(serialized);
        final String javaDeserializedJson = Serde.toPrettyJson(javaDeserialized);
        assertJsonEquals(inJson, javaDeserializedJson);
      }

      {
        final ISerializable javaOutObj = Serde.fromJson(inJson, ISerializable.class);
        final String javaOutJson = Serde.toPrettyJson(javaOutObj);
        assertJsonEquals(inJson, javaOutJson);
      }

      try (final ISerializableCo inObjCo = session.iSerializableOps().asCpp(inObj)) {
        final ISerializable cppOutObj = inObjCo.asJava();
        final String cppOutJson = Serde.toPrettyJson(cppOutObj);
        assertJsonEquals(inJson, cppOutJson);
        return new ObjectAndJson<>(cppOutObj, cppOutJson);
      }
    }
  }

  public static <T extends Variant> ObjectAndJson<Variant> testVariantRoundTrip(T inObj) {
    try (final MemoryManager memoryManager = MemoryManager.create(AllocationListener.NOOP);
        final LocalSession session = JniApiTests.createLocalSession(memoryManager)) {
      final String inJson = Serde.toPrettyJson(inObj);

      {
        final byte[] serialized = serialize(inObj);
        final Variant javaDeserialized = deserialize(serialized);
        final String javaDeserializedJson = Serde.toPrettyJson(javaDeserialized);
        assertJsonEquals(inJson, javaDeserializedJson);
      }

      {
        final Variant javaOutObj = Serde.fromJson(inJson, Variant.class);
        final String javaOutJson = Serde.toPrettyJson(javaOutObj);
        assertJsonEquals(inJson, javaOutJson);
      }

      try (final VariantCo inObjCo = session.variantOps().asCpp(inObj)) {
        final Variant cppOutObj = inObjCo.asJava();
        final String cppOutJson = Serde.toPrettyJson(cppOutObj);
        Assert.assertEquals(inObj, cppOutObj);
        assertJsonEquals(inJson, cppOutJson);
        return new ObjectAndJson<>(cppOutObj, cppOutJson);
      }
    }
  }

  public static <T> ObjectAndJson<Object> testJavaBeanRoundTrip(T inObj) {
    try {
      if (inObj instanceof NativeBean) {
        throw new VeloxException("Cannot round trip NativeBean");
      }
      final ObjectMapper jsonMapper = Serde.jsonMapper();
      final String inJson = jsonMapper.writeValueAsString(inObj);

      {
        final byte[] serialized = serialize((Serializable) inObj);
        final Object javaDeserialized = deserialize(serialized);
        final String javaDeserializedJson = jsonMapper.writeValueAsString(javaDeserialized);
        assertJsonEquals(inJson, javaDeserializedJson);
      }

      final Class<?> clazz = inObj.getClass();
      final Object outObj = jsonMapper.readValue(inJson, clazz);
      final String outJson = jsonMapper.writeValueAsString(outObj);
      assertJsonEquals(inJson, outJson);
      return new ObjectAndJson<>(outObj, outJson);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static HiveColumnHandle newSampleHiveColumnHandle() {
    final Type dataType =
        ArrayType.create(
            MapType.create(
                new VarCharType(),
                new RowType(
                    ImmutableList.of("id", "description"),
                    ImmutableList.of(new BigIntType(), new VarCharType()))));
    final HiveColumnHandle handle =
        new HiveColumnHandle(
            "complex_type",
            ColumnType.REGULAR,
            dataType,
            dataType,
            ImmutableList.of("complex_type[1][\"foo\"].id", "complex_type[2][\"foo\"].id"));
    return handle;
  }

  public static HiveConnectorSplit newSampleHiveSplit() {
    return new HiveConnectorSplit(
        "connector-1",
        5,
        true,
        "path/to/file",
        FileFormat.ORC,
        1,
        100,
        ImmutableMap.of("key", "value"),
        1,
        new HiveBucketConversion(
            1,
            1,
            ImmutableList.of(
                new HiveColumnHandle(
                    "t",
                    ColumnType.REGULAR,
                    new IntegerType(),
                    new IntegerType(),
                    Collections.emptyList()))),
        ImmutableMap.of("sk", "sv"),
        "extra",
        ImmutableMap.of("serde_key", "serde_value"),
        ImmutableMap.of("info_key", "info_value"),
        new FileProperties(100L, 50L),
        new RowIdProperties(5, 10, "UUID-100"));
  }

  public static HiveConnectorSplit newSampleHiveSplitWithMissingFields() {
    return new HiveConnectorSplit(
        "connector-1",
        5,
        true,
        "path/to/file",
        FileFormat.ORC,
        1,
        100,
        ImmutableMap.of("key", "value"),
        1,
        new HiveBucketConversion(
            1,
            1,
            ImmutableList.of(
                new HiveColumnHandle(
                    "t",
                    ColumnType.REGULAR,
                    new IntegerType(),
                    new IntegerType(),
                    Collections.emptyList()))),
        ImmutableMap.of("sk", "sv"),
        null,
        ImmutableMap.of("serde_key", "serde_value"),
        ImmutableMap.of("info_key", "info_value"),
        new FileProperties(null, 50L),
        new RowIdProperties(5, 10, "UUID-100"));
  }

  public static ConnectorTableHandle newSampleHiveTableHandle(RowType outputType) {
    final ConnectorTableHandle handle =
        new HiveTableHandle(
            "connector-1",
            "tab-1",
            true,
            ImmutableList.of(new SubfieldFilter("complex_type[1].id", new AlwaysTrue())),
            new CallTypedExpr(new BooleanType(), Collections.emptyList(), "always_true"),
            outputType,
            ImmutableMap.of("tk", "tv"));
    return handle;
  }

  public static LocationHandle newSampleLocationHandle() {
    return new LocationHandle(
        "/tmp/target-path",
        "/tmp/write-path",
        LocationHandle.TableType.EXISTING,
        "target-file-name");
  }

  public static HiveBucketProperty newSampleHiveBucketProperty() {
    return new HiveBucketProperty(
        HiveBucketProperty.Kind.PRESTO_NATIVE,
        10,
        ImmutableList.of("foo", "bar"),
        ImmutableList.of(new IntegerType(), new VarCharType()),
        ImmutableList.of(
            new HiveSortingColumn("foo", new SortOrder(true, true)),
            new HiveSortingColumn("bar", new SortOrder(false, false))));
  }

  public static HiveInsertTableHandle newSampleHiveInsertTableHandle() {
    return new HiveInsertTableHandle(
        ImmutableList.of(newSampleHiveColumnHandle()),
        newSampleLocationHandle(),
        FileFormat.PARQUET,
        newSampleHiveBucketProperty(),
        CompressionKind.ZLIB,
        ImmutableMap.of("serde_key", "serde_value"),
        false,
        new HiveInsertFileNameGenerator());
  }

  public static RowType newSampleOutputType() {
    return new RowType(
        ImmutableList.of("foo", "bar"), ImmutableList.of(new IntegerType(), new IntegerType()));
  }

  public static PlanNode newSampleTableScanNode(String planNodeId, RowType outputType) {
    final ConnectorTableHandle handle = SerdeTests.newSampleHiveTableHandle(outputType);
    final PlanNode scan =
        new TableScanNode(planNodeId, outputType, handle, Collections.emptyList());
    return scan;
  }

  public static Aggregate newSampleAggregate() {
    final Aggregate aggregate =
        new Aggregate(
            new CallTypedExpr(
                new IntegerType(),
                Collections.singletonList(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
                "sum"),
            ImmutableList.of(new IntegerType()),
            FieldAccessTypedExpr.create(new IntegerType(), "foo"),
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
            ImmutableList.of(new SortOrder(true, true)),
            true);
    return aggregate;
  }

  public static AggregationNode newSampleAggregationNode(String aggNodeId, String scanNodeId) {
    final PlanNode scan =
        SerdeTests.newSampleTableScanNode(scanNodeId, SerdeTests.newSampleOutputType());
    final Aggregate aggregate = SerdeTests.newSampleAggregate();
    final AggregationNode aggregationNode =
        new AggregationNode(
            aggNodeId,
            AggregateStep.PARTIAL,
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
            ImmutableList.of("sum"),
            ImmutableList.of(aggregate),
            true,
            ImmutableList.of(scan),
            FieldAccessTypedExpr.create(new IntegerType(), "foo"),
            ImmutableList.of(0));
    return aggregationNode;
  }

  public static WindowFunction newSampleWindowFunction() {
    final CallTypedExpr call =
        new CallTypedExpr(
            new IntegerType(),
            Collections.singletonList(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
            "sum");
    final WindowFrame frame =
        new WindowFrame(
            WindowType.ROWS, BoundType.UNBOUNDED_PRECEDING, null, BoundType.CURRENT_ROW, null);
    return new WindowFunction(call, frame, true);
  }

  public static WindowNode newSampleWindowNode(String windowNodeId, String scanNodeId) {
    final RowType rowType = SerdeTests.newSampleOutputType();
    final PlanNode scan = SerdeTests.newSampleTableScanNode(scanNodeId, rowType);
    final CallTypedExpr call =
        new CallTypedExpr(
            new IntegerType(),
            Collections.singletonList(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
            "sum");
    final WindowFrame frame =
        new WindowFrame(WindowType.RANGE, BoundType.PRECEDING, null, BoundType.FOLLOWING, null);
    return new WindowNode(
        windowNodeId,
        ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "bar")),
        ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
        ImmutableList.of(new SortOrder(true, false)),
        ImmutableList.of("sum_out"),
        ImmutableList.of(new WindowFunction(call, frame, true)),
        true,
        ImmutableList.of(scan));
  }

  public static class ObjectAndJson<T> {
    private final T obj;
    private final String json;

    private ObjectAndJson(T obj, String json) {
      this.obj = obj;
      this.json = json;
    }

    public T getObj() {
      return obj;
    }

    public String getJson() {
      return json;
    }
  }

  private static byte[] serialize(final Serializable obj) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
    try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
      out.writeObject(obj);
    } catch (IOException e) {
      throw new VeloxException(e);
    }
    return baos.toByteArray();
  }

  private static <T> T deserialize(final byte[] objectData) {
    Preconditions.checkNotNull(objectData, "objectData");
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(objectData))) {
      @SuppressWarnings("unchecked")
      final T obj = (T) in.readObject();
      return obj;
    } catch (final ClassNotFoundException | IOException | NegativeArraySizeException ex) {
      throw new VeloxException(ex);
    }
  }
}
