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

package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.stream.Streams;
import io.github.zhztheplayer.velox4j.test.Resources;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.util.List;
import java.util.stream.Collectors;

public class JniApiTest {
  public static final String QUERY_PATH = "query/example-1.json";
  public static final String QUERY_OUTPUT_PATH = "query-output/example-1.tsv";

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.initialize();
  }

  @Test
  public void testCreateAndClose() {
    final JniApi jniApi = JniApi.create();
    jniApi.close();
  }

  @Test
  public void testCreateTwice() {
    final JniApi jniApi1 = JniApi.create();
    final JniApi jniApi2 = JniApi.create();
    jniApi1.close();
    jniApi2.close();
  }

  @Test
  public void testCloseTwice() {
    final JniApi jniApi = JniApi.create();
    jniApi.close();
    Assert.assertThrows(VeloxException.class, new ThrowingRunnable() {
      @Override
      public void run() {
        jniApi.close();
      }
    });
  }

  @Test
  public void testExecuteQueryTryRun() {
    final String json = readQueryJson();
    final JniApi jniApi = JniApi.create();
    final UpIterator itr = jniApi.executeQuery(json);
    itr.close();
    jniApi.close();
  }

  @Test
  public void testExecuteQuery() {
    final JniApi jniApi = JniApi.create();
    final String json = readQueryJson();
    final UpIterator itr = jniApi.executeQuery(json);
    assertIterator(itr);
    jniApi.close();
  }

  @Test
  public void testExecuteQueryTwice() {
    final JniApi jniApi = JniApi.create();
    final String json = readQueryJson();
    final UpIterator itr1 = jniApi.executeQuery(json);
    final UpIterator itr2 = jniApi.executeQuery(json);
    assertIterator(itr1);
    assertIterator(itr2);
    jniApi.close();
  }

  @Test
  public void testVectorSerde() {
    final JniApi jniApi = JniApi.create();
    final String json = readQueryJson();
    final UpIterator itr = jniApi.executeQuery(json);
    final RowVector vector = collectSingleVector(itr);
    final String serialized = jniApi.baseVectorSerialize(vector);
    final BaseVector deserialized = jniApi.baseVectorDeserialize(serialized);
    final String serializedSecond = jniApi.baseVectorSerialize(deserialized);
    Assert.assertEquals(serialized, serializedSecond);
    jniApi.close();
  }

  @Test
  public void testArrowRoundTrip() {
    final JniApi jniApi = JniApi.create();
    final String json = readQueryJson();
    final UpIterator itr = jniApi.executeQuery(json);
    final RowVector vector = collectSingleVector(itr);
    final String serialized = jniApi.baseVectorSerialize(vector);
    final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
      jniApi.baseVectorExportToArrow(vector, cSchema, cArray);
      final FieldVector arrowVector = Data.importVector(alloc, cArray, cSchema, null);
      try (final ArrowSchema cSchema1 = ArrowSchema.allocateNew(alloc);
          final ArrowArray cArray1 = ArrowArray.allocateNew(alloc)) {
        Data.exportVector(alloc, arrowVector, null, cArray1, cSchema1);
        final BaseVector imported = jniApi.arrowImportToBaseVector(cSchema1, cArray1);
        final String serializedImported = jniApi.baseVectorSerialize(imported);
        Assert.assertEquals(serialized, serializedImported);
      }
      arrowVector.close();
    }
    jniApi.close();
  }

  private static String readQueryJson() {
    return Resources.readResourceAsString(QUERY_PATH);
  }

  private void assertIterator(UpIterator itr) {
    final RowVector vector = collectSingleVector(itr);
    Assert.assertEquals(Resources.readResourceAsString(QUERY_OUTPUT_PATH),
        RowVectors.toString(new RootAllocator(), vector));
    vector.close();
    itr.close();
  }

  private static RowVector collectSingleVector(UpIterator itr) {
    final List<RowVector> vectors = collect(itr);
    Assert.assertEquals(1, vectors.size());
    return vectors.get(0);
  }

  private static List<RowVector> collect(UpIterator itr) {
    final List<RowVector> vectors = Streams.fromIterator(itr).collect(Collectors.toList());
    return vectors;
  }
}
