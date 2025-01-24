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
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class JniApiTest {
  public static final String PLAN_PATH = "plan/example-1.json";
  public static final String PLAN_OUTPUT_PATH = "plan-output/example-1.tsv";

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.initialize();
  }

  @Test
  public void testCreationAndClose() {
    final JniApi jniApi = JniApi.create();
    jniApi.close();
  }


  @Test
  public void testExecutePlanTryRun() {
    final String json = readPlanJson();
    final JniApi jniApi = JniApi.create();
    final UpIterator itr = jniApi.executePlan(json);
    itr.close();
    jniApi.close();
  }

  @Test
  public void testExecutePlan() {
    final JniApi jniApi = JniApi.create();
    final String json = readPlanJson();
    final UpIterator itr = jniApi.executePlan(json);
    assertIterator(itr);
    jniApi.close();
  }

  @Test
  public void testExecutePlanTwice() {
    final JniApi jniApi = JniApi.create();
    final String json = readPlanJson();
    final UpIterator itr1 = jniApi.executePlan(json);
    final UpIterator itr2 = jniApi.executePlan(json);
    assertIterator(itr1);
    assertIterator(itr2);
    jniApi.close();
  }

  private static String readPlanJson() {
    return Resources.readResourceAsString(PLAN_PATH);
  }

  private void assertIterator(UpIterator itr) {
    final List<RowVector> vectors = Iterators.asStream(itr).collect(Collectors.toList());
    Assert.assertEquals(1, vectors.size());
    Assert.assertEquals(Resources.readResourceAsString(PLAN_OUTPUT_PATH),
        RowVectors.toString(new RootAllocator(), vectors.get(0)));
    vectors.forEach(RowVector::close);
    itr.close();
  }
}
