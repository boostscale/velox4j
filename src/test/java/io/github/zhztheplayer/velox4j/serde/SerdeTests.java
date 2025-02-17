package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.serializable.VeloxSerializable;
import io.github.zhztheplayer.velox4j.connector.ColumnHandle;
import io.github.zhztheplayer.velox4j.connector.ColumnType;
import io.github.zhztheplayer.velox4j.connector.ConnectorTableHandle;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.FileProperties;
import io.github.zhztheplayer.velox4j.connector.HiveBucketConversion;
import io.github.zhztheplayer.velox4j.connector.HiveColumnHandle;
import io.github.zhztheplayer.velox4j.connector.HiveConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.HiveTableHandle;
import io.github.zhztheplayer.velox4j.connector.RowIdProperties;
import io.github.zhztheplayer.velox4j.connector.SubfieldFilter;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.BaseVectors;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.filter.AlwaysTrue;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.sort.SortOrder;
import io.github.zhztheplayer.velox4j.test.ResourceTests;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.MapType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.Variant;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.Assert;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

public final class SerdeTests {
  private static void assertJsonEquals(String expected, String actual) {
    Assert.assertEquals(Serde.parseTree(expected), Serde.parseTree(actual));
    Assert.assertEquals(expected, actual);
  }

  public static <T extends VeloxSerializable> ObjectAndJson<T> testVeloxSerializableRoundTrip(T inObj) {
    try (final MemoryManager memoryManager = MemoryManager.create(AllocationListener.NOOP);
        final JniApi jniApi = JniApi.create(memoryManager)) {
      final String inJson = Serde.toPrettyJson(inObj);
      final String outJson = jniApi.deserializeAndSerialize(inJson);
      final VeloxSerializable outObj = Serde.fromJson(outJson, VeloxSerializable.class);
      final String outJson2 = Serde.toPrettyJson(outObj);
      assertJsonEquals(inJson, outJson2);
      return new ObjectAndJson<>((T) outObj, outJson2);
    }
  }

  public static <T extends VeloxSerializable> ObjectAndJson<T> testVeloxSerializableRoundTrip(String inJson,
      Class<? extends T> valueType) {
    final T inObj = Serde.fromJson(inJson, valueType);
    return SerdeTests.testVeloxSerializableRoundTrip(inObj);
  }

  public static <T extends Variant> ObjectAndJson<T> testVariantRoundTrip(T inObj) {
    try (final MemoryManager memoryManager = MemoryManager.create(AllocationListener.NOOP);
        final JniApi jniApi = JniApi.create(memoryManager)) {
      final String inJson = Serde.toPrettyJson(inObj);
      final String outJson = jniApi.deserializeAndSerializeVariant(inJson);
      final Variant outObj = Serde.fromJson(outJson, Variant.class);
      final String outJson2 = Serde.toPrettyJson(outObj);
      Assert.assertEquals(inObj, outObj);
      assertJsonEquals(inJson, outJson2);
      return new ObjectAndJson<>((T) outObj, outJson2);
    }
  }

  public static <T extends Object> ObjectAndJson<T> testJavaBeanRoundTrip(T inObj) {
    try {
      if (inObj instanceof NativeBean) {
        throw new VeloxException("Cannot round trip NativeBean");
      }
      final Class<?> clazz = inObj.getClass();
      final ObjectMapper jsonMapper = Serde.jsonMapper();
      final String inJson = jsonMapper.writeValueAsString(inObj);
      final Object outObj = jsonMapper.readValue(inJson, clazz);
      final String outJson = jsonMapper.writeValueAsString(outObj);
      assertJsonEquals(inJson, outJson);
      return new ObjectAndJson<>((T) outObj, outJson);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static ColumnHandle newSampleHiveColumnHandle() {
    final Type dataType = ArrayType.create(
        MapType.create(
            new VarCharType(),
            new RowType(List.of("id", "description"),
                List.of(new BigIntType(),
                    new VarCharType()))));
    final ColumnHandle handle = new HiveColumnHandle("complex_type",
        ColumnType.REGULAR, dataType, dataType, List.of(
        "complex_type[1][\"foo\"].id",
        "complex_type[2][\"foo\"].id"));
    return handle;
  }

  public static HiveConnectorSplit newSampleHiveSplit() {
    return new HiveConnectorSplit(
        "id-1",
        5,
        true,
        "path/to/file",
        FileFormat.ORC,
        1,
        100,
        Map.of("key", Optional.of("value")),
        OptionalInt.of(1),
        Optional.of(new HiveBucketConversion(
            1, 1,
            List.of(
                new HiveColumnHandle(
                    "t", ColumnType.REGULAR,
                    new IntegerType(), new IntegerType(), Collections.emptyList())))),
        Map.of("sk", "sv"),
        Optional.of("extra"),
        Map.of("serde_key", "serde_value"),
        Map.of("info_key", "info_value"),
        Optional.of(new FileProperties(OptionalLong.of(100), OptionalLong.of(50))),
        Optional.of(new RowIdProperties(5, 10, "UUID-100")));
  }

  public static HiveConnectorSplit newSampleHiveSplitWithMissingFields() {
    return new HiveConnectorSplit(
        "id-1",
        5,
        true,
        "path/to/file",
        FileFormat.ORC,
        1,
        100,
        Map.of("key", Optional.of("value")),
        OptionalInt.of(1),
        Optional.of(new HiveBucketConversion(
            1, 1,
            List.of(
                new HiveColumnHandle(
                    "t", ColumnType.REGULAR,
                    new IntegerType(), new IntegerType(), Collections.emptyList())))),
        Map.of("sk", "sv"),
        Optional.empty(),
        Map.of("serde_key", "serde_value"),
        Map.of("info_key", "info_value"),
        Optional.of(new FileProperties(OptionalLong.empty(), OptionalLong.of(50))),
        Optional.of(new RowIdProperties(5, 10, "UUID-100")));
  }

  public static ConnectorTableHandle newSampleHiveTableHandle(RowType outputType) {
    final ConnectorTableHandle handle = new HiveTableHandle(
        "id-1",
        "tab-1",
        true,
        List.of(new SubfieldFilter("complex_type[1].id", new AlwaysTrue())),
        new CallTypedExpr(new BooleanType(), Collections.emptyList(), "always_true"),
        outputType,
        Map.of("tk", "tv")
    );
    return handle;
  }

  public static Aggregate newSampleAggregate() {
    final Aggregate aggregate = new Aggregate(
        new CallTypedExpr(new IntegerType(),
            Collections.singletonList(FieldAccessTypedExpr.create(
                new IntegerType(), "foo")), "sum"),
        List.of(new IntegerType()),
        FieldAccessTypedExpr.create(new IntegerType(), "foo"),
        List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
        List.of(new SortOrder(true, true)), true);
    return aggregate;
  }

  public static PlanNode newSampleTableScanNode(String planNodeId, RowType outputType) {
    final ConnectorTableHandle handle = SerdeTests.newSampleHiveTableHandle(outputType);
    final PlanNode scan = new TableScanNode(planNodeId, outputType,
        handle, Collections.emptyList());
    return scan;
  }

  public static RowType newSampleOutputType() {
    return new RowType(List.of("foo", "bar"), List.of(new IntegerType(), new IntegerType()));
  }

  public static BaseVector newSampleIntVector(JniApi jniApi) {
    final BufferAllocator alloc = new RootAllocator();
    final IntVector arrowVector = new IntVector("foo", alloc);
    arrowVector.setValueCount(1);
    arrowVector.set(0, 15);
    final BaseVector baseVector = Arrow.fromArrowVector(jniApi, alloc, arrowVector);
    arrowVector.close();
    return baseVector;
  }

  public static RowVector newSampleRowVector(JniApi jniApi) {
    final String serialized = ResourceTests.readResourceAsString("vector/rowvector-1.b64");
    final BaseVector deserialized = BaseVectors.deserialize(jniApi, serialized);
    return ((RowVector) deserialized);
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
}
