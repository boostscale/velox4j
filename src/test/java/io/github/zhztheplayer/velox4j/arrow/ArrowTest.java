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
package io.github.zhztheplayer.velox4j.arrow;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.*;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.BaseVectorTests;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.memory.BytesAllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.test.TypeTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.*;

public class ArrowTest {
  private static BytesAllocationListener allocationListener;
  private static MemoryManager memoryManager;
  private static Session session;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    allocationListener = new BytesAllocationListener();
    memoryManager = MemoryManager.create(allocationListener);
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
  public void testBaseVectorRoundTrip() {
    final BaseVector input = BaseVectorTests.newSampleIntVector(session);
    final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    final FieldVector arrowVector = Arrow.toArrowVector(alloc, input);
    final BaseVector imported = session.arrowOps().fromArrowVector(alloc, arrowVector);
    BaseVectorTests.assertEquals(input, imported);
    arrowVector.close();
  }

  @Test
  public void testRowVectorRoundTrip1() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    final Table arrowTable = Arrow.toArrowTable(alloc, input);
    final RowVector imported = session.arrowOps().fromArrowTable(alloc, arrowTable);
    BaseVectorTests.assertEquals(input, imported);
    arrowTable.close();
  }

  @Test
  public void testRowVectorRoundTrip2() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    final Table arrowTable = Arrow.toArrowTable(alloc, input);
    final RowVector imported = session.arrowOps().fromArrowTable(alloc, arrowTable);
    BaseVectorTests.assertEquals(input, imported);
    arrowTable.close();
  }

  @Test
  public void testPrimitiveTypeRoundTrip() {
    final Type type = new DecimalType(10, 7);
    final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    final Field arrowField = session.arrowOps().toArrowField(alloc, type);
    final Type imported = Arrow.fromArrowField(alloc, arrowField);
    TypeTests.assertEquals(type, imported);
  }

  @Test
  public void testRowTypeRoundTrip1() {
    final RowType type =
        new RowType(
            ImmutableList.of("a", "B", "c", "D"),
            ImmutableList.of(
                new IntegerType(), new VarCharType(), new DecimalType(10, 7), new TimestampType()));
    final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    final Field arrowField = session.arrowOps().toArrowField(alloc, type);
    final Type imported = Arrow.fromArrowField(alloc, arrowField);
    TypeTests.assertEquals(type, imported);
  }

  @Test
  public void testRowTypeRoundTrip2() {
    final RowType type =
        new RowType(
            ImmutableList.of("a", "B", "c", "D"),
            ImmutableList.of(
                new IntegerType(), new VarCharType(), new DecimalType(10, 7), new TimestampType()));
    final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    final Schema arrowSchema = session.arrowOps().toArrowSchema(alloc, type);
    final RowType imported = Arrow.fromArrowSchema(alloc, arrowSchema);
    TypeTests.assertEquals(type, imported);
  }
}
