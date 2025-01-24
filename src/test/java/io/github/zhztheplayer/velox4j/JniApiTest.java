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

package io.github.zhztheplayer.velox4j;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.test.Iterators;
import io.github.zhztheplayer.velox4j.test.Resources;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.table.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class JniApiTest {
  @Before
  public void setUp() throws Exception {
    Velox4j.initialize();
  }

  @Test
  public void testExecutePlanTryRun() {
    final String json = Resources.readResourceAsString("plan/example-1.json");
    final JniApi jniApi = JniApi.create();
    final UpIterator itr = jniApi.executePlan(json);
    itr.close();
    jniApi.close();
  }

  @Test
  public void testExecutePlanTwice() {
    final JniApi jniApi = JniApi.create();
    final String json = Resources.readResourceAsString("plan/example-1.json");
    final UpIterator itr1 = jniApi.executePlan(json);
    final UpIterator itr2 = jniApi.executePlan(json);
    itr1.close();
    itr2.close();
    jniApi.close();
  }

  @Test
  public void testExecutePlanToArrow() {
    final JniApi jniApi = JniApi.create();
    final String json = Resources.readResourceAsString("plan/example-1.json");
    final UpIterator itr = jniApi.executePlan(json);
    final List<RowVector> vectors = Iterators.asStream(itr).collect(Collectors.toList());
    Assert.assertEquals(1, vectors.size());
    final Table arrowTable = RowVectors.toArrowTable(new RootAllocator(), vectors.get(0));
    arrowTable.close();
    vectors.forEach(RowVector::close);
    itr.close();
    jniApi.close();
  }
}
