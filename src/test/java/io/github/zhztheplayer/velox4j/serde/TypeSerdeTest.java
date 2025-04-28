/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.github.zhztheplayer.velox4j.serde;

import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class TypeSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testBoolean() {
    SerdeTests.testISerializableRoundTrip(new BooleanType());
  }

  @Test
  public void testTinyInt() {
    SerdeTests.testISerializableRoundTrip(new TinyIntType());
  }

  @Test
  public void testSmallInt() {
    SerdeTests.testISerializableRoundTrip(new SmallIntType());
  }

  @Test
  public void testInteger() {
    SerdeTests.testISerializableRoundTrip(new IntegerType());
  }

  @Test
  public void testBigInt() {
    SerdeTests.testISerializableRoundTrip(new BigIntType());
  }

  @Test
  public void testHugeInt() {
    SerdeTests.testISerializableRoundTrip(new HugeIntType());
  }

  @Test
  public void testRealType() {
    SerdeTests.testISerializableRoundTrip(new RealType());
  }

  @Test
  public void testDoubleType() {
    SerdeTests.testISerializableRoundTrip(new DoubleType());
  }

  @Test
  public void testVarcharType() {
    SerdeTests.testISerializableRoundTrip(new VarCharType());
  }

  @Test
  public void testVarbinaryType() {
    SerdeTests.testISerializableRoundTrip(new VarbinaryType());
  }

  @Test
  public void testTimestampType() {
    SerdeTests.testISerializableRoundTrip(new TimestampType());
  }

  @Test
  public void testArrayType() {
    SerdeTests.testISerializableRoundTrip(ArrayType.create(new IntegerType()));
  }

  @Test
  public void testMapType() {
    SerdeTests.testISerializableRoundTrip(MapType.create(new IntegerType(), new VarCharType()));
  }

  @Test
  public void testRowType() {
    SerdeTests.testISerializableRoundTrip(
        new RowType(List.of("foo", "bar"), List.of(new IntegerType(), new VarCharType())));
  }

  @Test
  public void testFunctionType() {
    SerdeTests.testISerializableRoundTrip(
        FunctionType.create(List.of(new IntegerType(), new VarCharType()), new VarbinaryType()));
  }

  @Test
  public void testUnknownType() {
    SerdeTests.testISerializableRoundTrip(new UnknownType());
  }

  @Test
  public void testOpaqueType() {
    Assert.assertThrows(
        VeloxException.class, () -> SerdeTests.testISerializableRoundTrip(new OpaqueType("foo")));
  }

  @Test
  public void testDecimalType() {
    SerdeTests.testISerializableRoundTrip(new DecimalType(10, 5));
  }

  @Test
  public void testIntervalDayTimeType() {
    SerdeTests.testISerializableRoundTrip(new IntervalDayTimeType());
  }

  @Test
  public void testIntervalYearMonthType() {
    SerdeTests.testISerializableRoundTrip(new IntervalYearMonthType());
  }

  @Test
  public void testDateType() {
    SerdeTests.testISerializableRoundTrip(new DateType());
  }
}
