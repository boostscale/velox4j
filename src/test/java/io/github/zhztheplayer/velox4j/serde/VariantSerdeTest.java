package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.variant.ArrayValue;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.BooleanValue;
import io.github.zhztheplayer.velox4j.variant.DoubleValue;
import io.github.zhztheplayer.velox4j.variant.HugeIntValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.MapValue;
import io.github.zhztheplayer.velox4j.variant.RealValue;
import io.github.zhztheplayer.velox4j.variant.RowValue;
import io.github.zhztheplayer.velox4j.variant.SmallIntValue;
import io.github.zhztheplayer.velox4j.variant.TimestampValue;
import io.github.zhztheplayer.velox4j.variant.TinyIntValue;
import io.github.zhztheplayer.velox4j.variant.VarBinaryValue;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class VariantSerdeTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testBooleanValue() {
    SerdeTests.testVariantRoundTrip(new BooleanValue(false));
    SerdeTests.testVariantRoundTrip(new BooleanValue(true));
  }

  @Test
  public void testTinyIntValue() {
    SerdeTests.testVariantRoundTrip(new TinyIntValue(-5));
    SerdeTests.testVariantRoundTrip(new TinyIntValue(0));
    SerdeTests.testVariantRoundTrip(new TinyIntValue(5));
  }

  @Test
  public void testSmallIntValue() {
    SerdeTests.testVariantRoundTrip(new SmallIntValue(-5));
    SerdeTests.testVariantRoundTrip(new SmallIntValue(0));
    SerdeTests.testVariantRoundTrip(new SmallIntValue(5));
  }

  @Test
  public void testIntegerValue() {
    SerdeTests.testVariantRoundTrip(new IntegerValue(-5));
    SerdeTests.testVariantRoundTrip(new IntegerValue(0));
    SerdeTests.testVariantRoundTrip(new IntegerValue(5));
  }

  @Test
  public void testBigIntValue() {
    SerdeTests.testVariantRoundTrip(new BigIntValue(-5L));
    SerdeTests.testVariantRoundTrip(new BigIntValue(0L));
    SerdeTests.testVariantRoundTrip(new BigIntValue(5L));
    SerdeTests.testVariantRoundTrip(new BigIntValue(Long.MAX_VALUE));
  }

  @Test
  public void testHugeIntValue() {
    final BigInteger int64Max = BigInteger.valueOf(Long.MAX_VALUE);
    final BigInteger plusOne = int64Max.add(BigInteger.valueOf(1));
    SerdeTests.testVariantRoundTrip(new HugeIntValue(int64Max));
    // FIXME this doesn't work
    Assert.assertThrows(VeloxException.class,
        () -> SerdeTests.testVariantRoundTrip(new HugeIntValue(plusOne)));
  }

  @Test
  public void testRealValue() {
    SerdeTests.testVariantRoundTrip(new RealValue(-5.5f));
    SerdeTests.testVariantRoundTrip(new RealValue(5.5f));
  }

  @Test
  public void testDoubleValue() {
    SerdeTests.testVariantRoundTrip(new DoubleValue(-5.5d));
    SerdeTests.testVariantRoundTrip(new DoubleValue(5.5d));
  }

  @Test
  public void testVarCharValue() {
    SerdeTests.testVariantRoundTrip(new VarCharValue("foo"));
  }

  @Test
  public void testVarBinaryValue() {
    final VarBinaryValue in = VarBinaryValue.create("foo".getBytes());
    SerdeTests.testVariantRoundTrip(in);
  }

  @Test
  public void testTimestampValue() {
    long seconds = System.currentTimeMillis() / 1000;
    long nanos = System.nanoTime() % 1_000_000_000L;
    final TimestampValue in = TimestampValue.create(seconds, nanos);
    SerdeTests.testVariantRoundTrip(in);
  }

  @Test
  public void testArrayValue() {
    SerdeTests.testVariantRoundTrip(new ArrayValue(
        List.of(new IntegerValue(100), new IntegerValue(500))));
    SerdeTests.testVariantRoundTrip(new ArrayValue(
        List.of(new BooleanValue(false), new BooleanValue(true))));
  }

  @Test
  public void testMapValue() {
    SerdeTests.testVariantRoundTrip(new MapValue(Map.of(
        new IntegerValue(100), new BooleanValue(false),
        new IntegerValue(1000), new BooleanValue(true),
        new IntegerValue(400), new BooleanValue(false),
        new IntegerValue(800), new BooleanValue(false),
        new IntegerValue(200), new BooleanValue(true),
        new IntegerValue(500), new BooleanValue(true))));
  }

  @Test
  public void testRowValue() {
    SerdeTests.testVariantRoundTrip(new RowValue(
        List.of(new IntegerValue(100), new BooleanValue(true))));
    SerdeTests.testVariantRoundTrip(new RowValue(
        List.of(new IntegerValue(500), new BooleanValue(false))));
  }
}
