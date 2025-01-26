package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.connector.ColumnHandle;
import io.github.zhztheplayer.velox4j.connector.ColumnType;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.FileProperties;
import io.github.zhztheplayer.velox4j.connector.HiveColumnHandle;
import io.github.zhztheplayer.velox4j.connector.RowIdProperties;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.MapType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarcharType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
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
    Assert.assertEquals("1", json);
  }

  @Test
  public void testProperties() {
    final FileProperties in = new FileProperties(OptionalLong.of(100),
        OptionalLong.of(50));
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
}
