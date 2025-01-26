package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.split.FileFormat;
import io.github.zhztheplayer.velox4j.split.FileProperties;
import io.github.zhztheplayer.velox4j.split.RowIdProperties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.OptionalLong;

public class SplitSerdeTest {
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
}
