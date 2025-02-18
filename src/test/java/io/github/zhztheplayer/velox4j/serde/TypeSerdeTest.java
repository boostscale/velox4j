package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.DateType;
import io.github.zhztheplayer.velox4j.type.DecimalType;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.FunctionType;
import io.github.zhztheplayer.velox4j.type.HugeIntType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.IntervalDayTimeType;
import io.github.zhztheplayer.velox4j.type.IntervalYearMonthType;
import io.github.zhztheplayer.velox4j.type.MapType;
import io.github.zhztheplayer.velox4j.type.OpaqueType;
import io.github.zhztheplayer.velox4j.type.RealType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.SmallIntType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.TinyIntType;
import io.github.zhztheplayer.velox4j.type.UnknownType;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.type.VarbinaryType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class TypeSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testBoolean() {
    SerdeTests.testVeloxSerializableRoundTrip(new BooleanType());
  }

  @Test
  public void testTinyInt() {
    SerdeTests.testVeloxSerializableRoundTrip(new TinyIntType());
  }

  @Test
  public void testSmallInt() {
    SerdeTests.testVeloxSerializableRoundTrip(new SmallIntType());
  }

  @Test
  public void testInteger() {
    SerdeTests.testVeloxSerializableRoundTrip(new IntegerType());
  }

  @Test
  public void testBigInt() {
    SerdeTests.testVeloxSerializableRoundTrip(new BigIntType());
  }

  @Test
  public void testHugeInt() {
    SerdeTests.testVeloxSerializableRoundTrip(new HugeIntType());
  }

  @Test
  public void testRealType() {
    SerdeTests.testVeloxSerializableRoundTrip(new RealType());
  }

  @Test
  public void testDoubleType() {
    SerdeTests.testVeloxSerializableRoundTrip(new DoubleType());
  }

  @Test
  public void testVarcharType() {
    SerdeTests.testVeloxSerializableRoundTrip(new VarCharType());
  }

  @Test
  public void testVarbinaryType() {
    SerdeTests.testVeloxSerializableRoundTrip(new VarbinaryType());
  }

  @Test
  public void testTimestampType() {
    SerdeTests.testVeloxSerializableRoundTrip(new TimestampType());
  }

  @Test
  public void testArrayType() {
    SerdeTests.testVeloxSerializableRoundTrip(ArrayType.create(new IntegerType()));
  }

  @Test
  public void testMapType() {
    SerdeTests.testVeloxSerializableRoundTrip(MapType.create(new IntegerType(), new VarCharType()));
  }

  @Test
  public void testRowType() {
    SerdeTests.testVeloxSerializableRoundTrip(new RowType(List.of("foo", "bar"),
        List.of(new IntegerType(), new VarCharType())));
  }

  @Test
  public void testFunctionType() {
    SerdeTests.testVeloxSerializableRoundTrip(FunctionType.create(List.of(
        new IntegerType(), new VarCharType()), new VarbinaryType()));
  }

  @Test
  public void testUnknownType() {
    SerdeTests.testVeloxSerializableRoundTrip(new UnknownType());
  }

  @Test
  public void testOpaqueType() {
    Assert.assertThrows(VeloxException.class, () -> SerdeTests.testVeloxSerializableRoundTrip(new OpaqueType("foo")));
  }

  @Test
  public void testDecimalType() {
    SerdeTests.testVeloxSerializableRoundTrip(new DecimalType(10, 5));
  }

  @Test
  public void testIntervalDayTimeType() {
    SerdeTests.testVeloxSerializableRoundTrip(new IntervalDayTimeType());
  }

  @Test
  public void testIntervalYearMonthType() {
    SerdeTests.testVeloxSerializableRoundTrip(new IntervalYearMonthType());
  }

  @Test
  public void testDateType() {
    SerdeTests.testVeloxSerializableRoundTrip(new DateType());
  }
}
