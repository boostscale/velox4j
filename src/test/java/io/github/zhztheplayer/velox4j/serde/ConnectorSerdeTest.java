package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.connector.Assignment;
import io.github.zhztheplayer.velox4j.connector.ColumnHandle;
import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ConnectorTableHandle;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.FileProperties;
import io.github.zhztheplayer.velox4j.connector.RowIdProperties;
import io.github.zhztheplayer.velox4j.connector.SubfieldFilter;
import io.github.zhztheplayer.velox4j.filter.AlwaysTrue;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.OptionalLong;

public class ConnectorSerdeTest {
  @BeforeClass
  public static void beforeClass() {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testFileFormat() {
    final FileFormat in = FileFormat.DWRF;
    final String json = SerdeTests.testJavaBeanRoundTrip(in).getJson();
    Assert.assertEquals("\"dwrf\"", json);
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
  public void testSubfieldFilter() {
    final SubfieldFilter in = new SubfieldFilter(
        "complex_type[1][\"foo\"].id", new AlwaysTrue());
    SerdeTests.testJavaBeanRoundTrip(in);
  }

  @Test
  public void testAssignment() {
    final Assignment assignment = new Assignment("foo", SerdeTests.newSampleHiveColumnHandle());
    SerdeTests.testJavaBeanRoundTrip(assignment);
  }

  @Test
  public void testRowIdProperties() {
    final RowIdProperties in = new RowIdProperties(
        5, 10, "UUID-100");
    SerdeTests.testJavaBeanRoundTrip(in);
  }

  @Test
  public void testHiveColumnHandle() {
    final ColumnHandle handle = SerdeTests.newSampleHiveColumnHandle();
    SerdeTests.testVeloxSerializableRoundTrip(handle);
  }

  @Test
  public void testHiveConnectorSplit() {
    final ConnectorSplit split = SerdeTests.newSampleHiveSplit();
    SerdeTests.testVeloxSerializableRoundTrip(split);
  }

  @Test
  public void testHiveConnectorSplitWithMissingFields() {
    final ConnectorSplit split = SerdeTests.newSampleHiveSplitWithMissingFields();
    SerdeTests.testVeloxSerializableRoundTrip(split);
  }

  @Test
  public void testHiveTableHandle() {
    final ConnectorTableHandle handle = SerdeTests.newSampleHiveTableHandle(SerdeTests.newSampleOutputType());
    SerdeTests.testVeloxSerializableRoundTrip(handle);
  }

  @Test
  public void testExternalStreamConnectorSplit() {
    final ConnectorSplit split = new ExternalStreamConnectorSplit("id-1", 100);
    SerdeTests.testVeloxSerializableRoundTrip(split);
  }

  @Test
  public void testExternalStreamTableHandle() {
    final ExternalStreamTableHandle handle = new ExternalStreamTableHandle("id-1");
    SerdeTests.testVeloxSerializableRoundTrip(handle);
  }
}
