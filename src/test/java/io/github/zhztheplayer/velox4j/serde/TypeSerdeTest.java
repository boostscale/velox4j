package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.type.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TypeSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testBoolean() {
    SerdeTests.testNativeObjectRoundTrip(new BooleanType());
  }

  @Test
  public void testTinyInt() {
    SerdeTests.testNativeObjectRoundTrip(new TinyIntType());
  }

  @Test
  public void testSmallInt() {
    SerdeTests.testNativeObjectRoundTrip(new SmallIntType());
  }

  @Test
  public void testInteger() {
    SerdeTests.testNativeObjectRoundTrip(new IntegerType());
  }

  @Test
  public void testBigInt() {
    SerdeTests.testNativeObjectRoundTrip(new BigIntType());
  }

  @Test
  public void testHugeInt() {
    SerdeTests.testNativeObjectRoundTrip(new HugeIntType());
  }

  @Test
  public void testRealType() {
    SerdeTests.testNativeObjectRoundTrip(new RealType());
  }

  @Test
  public void testDoubleType() {
    SerdeTests.testNativeObjectRoundTrip(new DoubleType());
  }

  @Test
  public void testVarcharType() {
    SerdeTests.testNativeObjectRoundTrip(new VarcharType());
  }

  @Test
  public void testVarbinaryType() {
    SerdeTests.testNativeObjectRoundTrip(new VarbinaryType());
  }

  @Test
  public void testTimestampType() {
    SerdeTests.testNativeObjectRoundTrip(new TimestampType());
  }

  @Test
  public void testArrayType() {
    SerdeTests.testNativeObjectRoundTrip(ArrayType.create(new IntegerType()));
  }

  @Test
  public void testMapType() {
    SerdeTests.testNativeObjectRoundTrip(MapType.create(new IntegerType(), new VarcharType()));
  }

  @Test
  public void testRowType() {
    SerdeTests.testNativeObjectRoundTrip(new RowType(List.of("foo", "bar"),
        List.of(new IntegerType(), new VarcharType())));
  }

  @Test
  public void testFunctionType() {
    SerdeTests.testNativeObjectRoundTrip(FunctionType.create(List.of(
        new IntegerType(), new VarcharType()), new VarbinaryType()));
  }

  @Test
  public void testUnknownType() {
    SerdeTests.testNativeObjectRoundTrip(new UnknownType());
  }

  @Test
  public void testOpaqueType() {
    Assert.assertThrows(VeloxException.class, () -> SerdeTests.testNativeObjectRoundTrip(new OpaqueType("foo")));
  }

  @Test
  public void testDecimalType() {
    SerdeTests.testNativeObjectRoundTrip(new DecimalType(10, 5));
  }

  @Test
  public void testIntervalDayTimeType() {
    SerdeTests.testNativeObjectRoundTrip(new IntervalDayTimeType());
  }

  @Test
  public void testIntervalYearMonthType() {
    SerdeTests.testNativeObjectRoundTrip(new IntervalYearMonthType());
  }

  @Test
  public void testDateType() {
    SerdeTests.testNativeObjectRoundTrip(new DateType());
  }
}
