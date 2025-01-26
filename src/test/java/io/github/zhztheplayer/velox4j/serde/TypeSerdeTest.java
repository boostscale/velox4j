package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.test.Serdes;
import io.github.zhztheplayer.velox4j.type.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

public class TypeSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.initialize();
  }

  @Test
  public void testBoolean() {
    Serdes.testRoundTrip(new BooleanType());
  }

  @Test
  public void testTinyInt() {
    Serdes.testRoundTrip(new TinyIntType());
  }

  @Test
  public void testSmallInt() {
    Serdes.testRoundTrip(new SmallIntType());
  }

  @Test
  public void testInteger() {
    Serdes.testRoundTrip(new IntegerType());
  }

  @Test
  public void testBigInt() {
    Serdes.testRoundTrip(new BigIntType());
  }

  @Test
  public void testHugeInt() {
    Serdes.testRoundTrip(new HugeIntType());
  }

  @Test
  public void testRealType() {
    Serdes.testRoundTrip(new RealType());
  }

  @Test
  public void testDoubleType() {
    Serdes.testRoundTrip(new DoubleType());
  }

  @Test
  public void testVarcharType() {
    Serdes.testRoundTrip(new VarcharType());
  }

  @Test
  public void testVarbinaryType() {
    Serdes.testRoundTrip(new VarbinaryType());
  }

  @Test
  public void testTimestampType() {
    Serdes.testRoundTrip(new TimestampType());
  }

  @Test
  public void testArrayType() {
    Serdes.testRoundTrip(new ArrayType(Arrays.asList(new IntegerType())));
  }

  @Test
  public void testMapType() {
    Serdes.testRoundTrip(new MapType(Arrays.asList(new IntegerType(), new VarcharType())));
  }

  @Test
  public void testRowType() {
    Serdes.testRoundTrip(new RowType(Arrays.asList("foo", "bar"),
        Arrays.asList(new IntegerType(), new VarcharType())));
  }

  @Test
  public void testFunctionType() {
    Serdes.testRoundTrip(FunctionType.create(Arrays.asList(
        new IntegerType(), new VarcharType()), new VarbinaryType()));
  }

  @Test
  public void testUnknownType() {
    Serdes.testRoundTrip(new UnknownType());
  }

  @Test
  public void testOpaqueType() {
    Assert.assertThrows(VeloxException.class, () -> Serdes.testRoundTrip(new OpaqueType("foo")));
  }

  @Test
  public void testDecimalType() {
    Serdes.testRoundTrip(new DecimalType(10, 5));
  }

  @Test
  public void testIntervalDayTimeType() {
    Serdes.testRoundTrip(new IntervalDayTimeType());
  }

  @Test
  public void testIntervalYearMonthType() {
    Serdes.testRoundTrip(new IntervalYearMonthType());
  }

  @Test
  public void testDateType() {
    Serdes.testRoundTrip(new DateType());
  }
}
