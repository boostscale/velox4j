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
package io.github.zhztheplayer.velox4j.variant;

import java.util.Arrays;

import org.junit.*;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.memory.BytesAllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.test.ResourceTests;
import io.github.zhztheplayer.velox4j.test.TypeTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.VarCharType;

public class VariantTest {
  private static BytesAllocationListener allocationListener;
  private static MemoryManager memoryManager;
  private static Session session;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    allocationListener = new BytesAllocationListener();
    memoryManager = Velox4j.newMemoryManager(allocationListener);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    memoryManager.close();
    Assert.assertEquals(0, allocationListener.currentBytes());
  }

  @Before
  public void setUp() throws Exception {
    session = Velox4j.newSession(memoryManager);
  }

  @After
  public void tearDown() throws Exception {
    session.close();
  }

  @Test
  public void testPrimitiveInterType() {
    final IntegerValue value = new IntegerValue(-5);
    TypeTests.assertEquals(new IntegerType(), session.variantOps().inferType(value));
  }

  @Test
  public void testArrayInterType() {
    final ArrayValue value =
        new ArrayValue(Arrays.asList(new IntegerValue(10), new IntegerValue(-5)));
    TypeTests.assertEquals(
        ArrayType.create(new IntegerType()), session.variantOps().inferType(value));
  }

  @Test
  public void testPrimitiveVariantToVector() {
    final IntegerValue value = new IntegerValue(-5);
    final BaseVector vector = session.variantOps().toVector(new IntegerType(), value);
    Assert.assertEquals(
        ResourceTests.readResourceAsString("vector-output/variant-to-vector-1.txt"),
        vector.toString());
  }

  @Test
  public void testPrimitiveVariantToVectorWrongType() {
    final IntegerValue value = new IntegerValue(-5);
    Assert.assertThrows(
        VeloxException.class,
        () -> {
          session.variantOps().toVector(new VarCharType(), value);
        });
  }

  @Test
  public void testArrayVariantToVector() {
    final ArrayValue value =
        new ArrayValue(Arrays.asList(new IntegerValue(10), new IntegerValue(-5)));
    final BaseVector vector =
        session.variantOps().toVector(ArrayType.create(new IntegerType()), value);
    Assert.assertEquals(
        ResourceTests.readResourceAsString("vector-output/variant-to-vector-2.txt"),
        vector.toString());
  }
}
