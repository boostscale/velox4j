package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.connector.Assignment;
import io.github.zhztheplayer.velox4j.connector.ColumnHandle;
import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ConnectorTableHandle;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.FileProperties;
import io.github.zhztheplayer.velox4j.connector.RowIdProperties;
import io.github.zhztheplayer.velox4j.connector.SubfieldFilter;
import io.github.zhztheplayer.velox4j.filter.AlwaysTrue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.OptionalLong;

public class ConnectorSerdeTest {
  @BeforeClass
  public static void beforeClass() {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testFileFormat() {
    final FileFormat in = FileFormat.DWRF;
    final String json = SerdeTests.testJavaObjectRoundTrip(in);
    Assert.assertEquals("\"dwrf\"", json);
  }

  @Test
  public void testProperties() {
    final FileProperties in = new FileProperties(OptionalLong.of(100),
        OptionalLong.of(50));
    SerdeTests.testJavaObjectRoundTrip(in);
  }

  @Test
  public void testPropertiesWithMissingFields() {
    final FileProperties in = new FileProperties(OptionalLong.of(100),
        OptionalLong.empty());
    SerdeTests.testJavaObjectRoundTrip(in);
  }

  @Test
  public void testSubfieldFilter() {
    final SubfieldFilter in = new SubfieldFilter(
        "complex_type[1][\"foo\"].id", new AlwaysTrue());
    SerdeTests.testJavaObjectRoundTrip(in);
  }

  @Test
  public void testAssignment() {
    final Assignment assignment = new Assignment("foo", SerdeTests.newSampleHiveColumnHandle());
    SerdeTests.testJavaObjectRoundTrip(assignment);
  }

  @Test
  public void testRowIdProperties() {
    final RowIdProperties in = new RowIdProperties(
        5, 10, "UUID-100");
    SerdeTests.testJavaObjectRoundTrip(in);
  }

  @Test
  public void testHiveColumnHandle() {
    final ColumnHandle handle = SerdeTests.newSampleHiveColumnHandle();
    SerdeTests.testNativeObjectRoundTrip(handle);
  }

  @Test
  public void testHiveConnectorSplit() {
    final ConnectorSplit split = SerdeTests.newSampleHiveSplit();
    SerdeTests.testNativeObjectRoundTrip(split);
  }

  @Test
  public void testHiveConnectorSplitWithMissingFields() {
    final ConnectorSplit split = SerdeTests.newSampleHiveSplitWithMissingFields();
    SerdeTests.testNativeObjectRoundTrip(split);
  }

  @Test
  public void testHiveTableHandle() {
    final ConnectorTableHandle handle = SerdeTests.newSampleHiveTableHandle();
    SerdeTests.testNativeObjectRoundTrip(handle);
  }
}
