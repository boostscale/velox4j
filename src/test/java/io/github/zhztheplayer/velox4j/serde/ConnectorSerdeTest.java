package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.connector.ColumnHandle;
import io.github.zhztheplayer.velox4j.connector.ColumnType;
import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ConnectorTableHandle;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.FileProperties;
import io.github.zhztheplayer.velox4j.connector.HiveBucketConversion;
import io.github.zhztheplayer.velox4j.connector.HiveColumnHandle;
import io.github.zhztheplayer.velox4j.connector.HiveConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.HiveTableHandle;
import io.github.zhztheplayer.velox4j.connector.RowIdProperties;
import io.github.zhztheplayer.velox4j.connector.SubfieldFilter;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.filter.AlwaysTrue;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.MapType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarcharType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class ConnectorSerdeTest {
  @BeforeClass
  public static void beforeClass() {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testFileFormat() {
    final FileFormat in = FileFormat.DWRF;
    final String json = SerdeTests.testJavaBeanRoundTrip(in);
    Assert.assertEquals("dwrf", json);
  }

  @Test
  public void testProperties() {
    final FileProperties in = new FileProperties(OptionalLong.of(100),
        OptionalLong.of(50));
    SerdeTests.testJavaBeanRoundTrip(in);
  }

  @Test
  public void testSubfieldFilter() {
    final SubfieldFilter in = new SubfieldFilter(
        "complex_type[1][\"foo\"].id", new AlwaysTrue());
    SerdeTests.testJavaBeanRoundTrip(in);
  }

  @Test
  public void testPropertiesWithMissingFields() {
    final FileProperties in = new FileProperties(OptionalLong.of(100),
        OptionalLong.empty());
    SerdeTests.testJavaBeanRoundTrip(in);
  }

  @Test
  public void testRowIdProperties() {
    final RowIdProperties in = new RowIdProperties(
        5, 10, "UUID-100");
    SerdeTests.testJavaBeanRoundTrip(in);
  }

  @Test
  public void testHiveColumnHandle() {
    final Type dataType = ArrayType.create(
        MapType.create(
            new VarcharType(),
            new RowType(Arrays.asList("id", "description"),
                Arrays.asList(new BigIntType(),
                    new VarcharType()))));
    final ColumnHandle handle = new HiveColumnHandle("complex_type",
        ColumnType.REGULAR, dataType, dataType, Arrays.asList(
        "complex_type[1][\"foo\"].id",
        "complex_type[2][\"foo\"].id"));
    SerdeTests.testVeloxBeanRoundTrip(handle);
  }

  @Test
  public void testHiveConnectorSplit() {
    final ConnectorSplit split = newSampleHiveSplit();
    SerdeTests.testVeloxBeanRoundTrip(split);
  }

  @Test
  public void testHiveConnectorSplitWithMissingFields() {
    final ConnectorSplit split = newSampleHiveSplitWithMissingFields();
    SerdeTests.testVeloxBeanRoundTrip(split);
  }

  @Test
  public void testHiveTableHandle() {
    final ConnectorTableHandle handle = new HiveTableHandle(
        "id-1",
        "tab-1",
        true,
        Arrays.asList(new SubfieldFilter("complex_type[1].id", new AlwaysTrue())),
        new CallTypedExpr(new BooleanType(), Collections.emptyList(), "always_true"),
        new RowType(Arrays.asList("foo", "bar"), Arrays.asList(new VarcharType(), new VarcharType())),
        Map.of("tk", "tv")
    );
    SerdeTests.testVeloxBeanRoundTrip(handle);
  }

  private static HiveConnectorSplit newSampleHiveSplit() {
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
            Arrays.asList(
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

  private static HiveConnectorSplit newSampleHiveSplitWithMissingFields() {
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
            Arrays.asList(
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
}
