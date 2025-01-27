package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.type.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class TypeSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testBoolean() {
    SerdeTests.testNativeBeanRoundTrip(new BooleanType());
  }

  @Test
  public void testTinyInt() {
    SerdeTests.testNativeBeanRoundTrip(new TinyIntType());
  }

  @Test
  public void testSmallInt() {
    SerdeTests.testNativeBeanRoundTrip(new SmallIntType());
  }

  @Test
  public void testInteger() {
    SerdeTests.testNativeBeanRoundTrip(new IntegerType());
  }

  @Test
  public void testBigInt() {
    SerdeTests.testNativeBeanRoundTrip(new BigIntType());
  }

  @Test
  public void testHugeInt() {
    SerdeTests.testNativeBeanRoundTrip(new HugeIntType());
  }

  @Test
  public void testRealType() {
    SerdeTests.testNativeBeanRoundTrip(new RealType());
  }

  @Test
  public void testDoubleType() {
    SerdeTests.testNativeBeanRoundTrip(new DoubleType());
  }

  @Test
  public void testVarcharType() {
    SerdeTests.testNativeBeanRoundTrip(new VarcharType());
  }

  @Test
  public void testVarbinaryType() {
    SerdeTests.testNativeBeanRoundTrip(new VarbinaryType());
  }

  @Test
  public void testTimestampType() {
    SerdeTests.testNativeBeanRoundTrip(new TimestampType());
  }

  @Test
  public void testArrayType() {
    SerdeTests.testNativeBeanRoundTrip(ArrayType.create(new IntegerType()));
  }

  @Test
  public void testMapType() {
    SerdeTests.testNativeBeanRoundTrip(MapType.create(new IntegerType(), new VarcharType()));
  }

  @Test
  public void testRowType() {
    SerdeTests.testNativeBeanRoundTrip(new RowType(List.of("foo", "bar"),
        List.of(new IntegerType(), new VarcharType())));
  }

  @Test
  public void testFunctionType() {
    SerdeTests.testNativeBeanRoundTrip(FunctionType.create(List.of(
        new IntegerType(), new VarcharType()), new VarbinaryType()));
  }

  @Test
  public void testUnknownType() {
    SerdeTests.testNativeBeanRoundTrip(new UnknownType());
  }

  @Test
  public void testOpaqueType() {
    Assert.assertThrows(VeloxException.class, () -> SerdeTests.testNativeBeanRoundTrip(new OpaqueType("foo")));
  }

  @Test
  public void testDecimalType() {
    SerdeTests.testNativeBeanRoundTrip(new DecimalType(10, 5));
  }

  @Test
  public void testIntervalDayTimeType() {
    SerdeTests.testNativeBeanRoundTrip(new IntervalDayTimeType());
  }

  @Test
  public void testIntervalYearMonthType() {
    SerdeTests.testNativeBeanRoundTrip(new IntervalYearMonthType());
  }

  @Test
  public void testDateType() {
    SerdeTests.testNativeBeanRoundTrip(new DateType());
  }
}
