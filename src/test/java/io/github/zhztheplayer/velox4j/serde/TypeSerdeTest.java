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
    SerdeTests.testVeloxBeanRoundTrip(new BooleanType());
  }

  @Test
  public void testTinyInt() {
    SerdeTests.testVeloxBeanRoundTrip(new TinyIntType());
  }

  @Test
  public void testSmallInt() {
    SerdeTests.testVeloxBeanRoundTrip(new SmallIntType());
  }

  @Test
  public void testInteger() {
    SerdeTests.testVeloxBeanRoundTrip(new IntegerType());
  }

  @Test
  public void testBigInt() {
    SerdeTests.testVeloxBeanRoundTrip(new BigIntType());
  }

  @Test
  public void testHugeInt() {
    SerdeTests.testVeloxBeanRoundTrip(new HugeIntType());
  }

  @Test
  public void testRealType() {
    SerdeTests.testVeloxBeanRoundTrip(new RealType());
  }

  @Test
  public void testDoubleType() {
    SerdeTests.testVeloxBeanRoundTrip(new DoubleType());
  }

  @Test
  public void testVarcharType() {
    SerdeTests.testVeloxBeanRoundTrip(new VarcharType());
  }

  @Test
  public void testVarbinaryType() {
    SerdeTests.testVeloxBeanRoundTrip(new VarbinaryType());
  }

  @Test
  public void testTimestampType() {
    SerdeTests.testVeloxBeanRoundTrip(new TimestampType());
  }

  @Test
  public void testArrayType() {
    SerdeTests.testVeloxBeanRoundTrip(ArrayType.create(new IntegerType()));
  }

  @Test
  public void testMapType() {
    SerdeTests.testVeloxBeanRoundTrip(MapType.create(new IntegerType(), new VarcharType()));
  }

  @Test
  public void testRowType() {
    SerdeTests.testVeloxBeanRoundTrip(new RowType(List.of("foo", "bar"),
        List.of(new IntegerType(), new VarcharType())));
  }

  @Test
  public void testFunctionType() {
    SerdeTests.testVeloxBeanRoundTrip(FunctionType.create(List.of(
        new IntegerType(), new VarcharType()), new VarbinaryType()));
  }

  @Test
  public void testUnknownType() {
    SerdeTests.testVeloxBeanRoundTrip(new UnknownType());
  }

  @Test
  public void testOpaqueType() {
    Assert.assertThrows(VeloxException.class, () -> SerdeTests.testVeloxBeanRoundTrip(new OpaqueType("foo")));
  }

  @Test
  public void testDecimalType() {
    SerdeTests.testVeloxBeanRoundTrip(new DecimalType(10, 5));
  }

  @Test
  public void testIntervalDayTimeType() {
    SerdeTests.testVeloxBeanRoundTrip(new IntervalDayTimeType());
  }

  @Test
  public void testIntervalYearMonthType() {
    SerdeTests.testVeloxBeanRoundTrip(new IntervalYearMonthType());
  }

  @Test
  public void testDateType() {
    SerdeTests.testVeloxBeanRoundTrip(new DateType());
  }
}
