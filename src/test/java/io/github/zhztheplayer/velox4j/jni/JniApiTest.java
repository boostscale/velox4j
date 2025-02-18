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

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.test.SampleQueryTests;
import io.github.zhztheplayer.velox4j.test.UpIteratorTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RealType;
import io.github.zhztheplayer.velox4j.variant.DoubleValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.RealValue;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.util.Collections;
import java.util.List;

public class JniApiTest {
  private static MemoryManager memoryManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    memoryManager.close();
  }

  @Test
  public void testCreateAndClose() {
    final JniApi jniApi = JniApi.create(memoryManager);
    jniApi.close();
  }

  @Test
  public void testCreateTwice() {
    final JniApi jniApi1 = JniApi.create(memoryManager);
    final JniApi jniApi2 = JniApi.create(memoryManager);
    jniApi1.close();
    jniApi2.close();
  }

  @Test
  public void testCloseTwice() {
    final JniApi jniApi = JniApi.create(memoryManager);
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
    final String json = SampleQueryTests.readQueryJson();
    final JniApi jniApi = JniApi.create(memoryManager);
    final UpIterator itr = jniApi.executeQuery(json);
    itr.close();
    jniApi.close();
  }

  @Test
  public void testExecuteQuery() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator itr = jniApi.executeQuery(json);
    SampleQueryTests.assertIterator(itr);
    jniApi.close();
  }

  @Test
  public void testExecuteQueryTwice() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator itr1 = jniApi.executeQuery(json);
    final UpIterator itr2 = jniApi.executeQuery(json);
    SampleQueryTests.assertIterator(itr1);
    SampleQueryTests.assertIterator(itr2);
    jniApi.close();
  }

  @Test
  public void testVectorSerdeEmpty() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final String serialized = jniApi.baseVectorSerialize(Collections.emptyList());
    final List<BaseVector> deserialized = jniApi.baseVectorDeserialize(serialized);
    Assert.assertTrue(deserialized.isEmpty());
    final String serializedSecond = jniApi.baseVectorSerialize(deserialized);
    Assert.assertEquals(serialized, serializedSecond);
    jniApi.close();
  }

  @Test
  public void testVectorSerdeSingle() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator itr = jniApi.executeQuery(json);
    final RowVector vector = UpIteratorTests.collectSingleVector(itr);
    final String serialized = jniApi.baseVectorSerialize(List.of(vector));
    final List<BaseVector> deserialized = jniApi.baseVectorDeserialize(serialized);
    Assert.assertEquals(1, deserialized.size());
    final String serializedSecond = jniApi.baseVectorSerialize(deserialized);
    Assert.assertEquals(serialized, serializedSecond);
    jniApi.close();
  }


  @Test
  public void testVectorSerdeMultiple() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator itr = jniApi.executeQuery(json);
    final RowVector vector = UpIteratorTests.collectSingleVector(itr);
    final String serialized = jniApi.baseVectorSerialize(List.of(vector, vector));
    final List<BaseVector> deserialized = jniApi.baseVectorDeserialize(serialized);
    Assert.assertEquals(2, deserialized.size());
    final String serializedSecond = jniApi.baseVectorSerialize(deserialized);
    Assert.assertEquals(serialized, serializedSecond);
    jniApi.close();
  }

  @Test
  public void testArrowRoundTrip() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator itr = jniApi.executeQuery(json);
    final RowVector vector = UpIteratorTests.collectSingleVector(itr);
    final String serialized = jniApi.baseVectorSerialize(List.of(vector));
    final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    final FieldVector arrowVector = Arrow.toArrowVector(alloc, vector);
    final BaseVector imported = Arrow.fromArrowVector(jniApi, alloc, arrowVector);
    final String serializedImported = jniApi.baseVectorSerialize(List.of(imported));
    Assert.assertEquals(serialized, serializedImported);
    arrowVector.close();
    jniApi.close();
  }

  @Test
  public void testVariantInferType() {
    final JniApi jniApi = JniApi.create(memoryManager);
    Assert.assertTrue(jniApi.variantInferType(new IntegerValue(5)) instanceof IntegerType);
    Assert.assertTrue(jniApi.variantInferType(new RealValue(4.6f)) instanceof RealType);
    Assert.assertTrue(jniApi.variantInferType(new DoubleValue(4.6d)) instanceof DoubleType);
    jniApi.close();
  }

  @Test
  public void testIteratorRoundTrip() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator itr = jniApi.executeQuery(json);
    final DownIterator down = new DownIterator(itr);
    final ExternalStream es = jniApi.newExternalStream(down);
    final UpIterator up = jniApi.createUpIteratorWithExternalStream(es);
    SampleQueryTests.assertIterator(up);
    jniApi.close();
  }

  @Test
  public void testIteratorRoundTripInDifferentThread() throws InterruptedException {
    final JniApi jniApi = JniApi.create(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator itr = jniApi.executeQuery(json);
    final DownIterator down = new DownIterator(itr);
    final ExternalStream es = jniApi.newExternalStream(down);
    final UpIterator up = jniApi.createUpIteratorWithExternalStream(es);
    final Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        SampleQueryTests.assertIterator(up);
      }
    });
    thread.start();
    thread.join();
    jniApi.close();
  }
}
