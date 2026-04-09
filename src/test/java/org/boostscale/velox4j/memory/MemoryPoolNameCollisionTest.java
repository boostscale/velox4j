/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.boostscale.velox4j.memory;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.boostscale.velox4j.Velox4j;
import org.boostscale.velox4j.arrow.Arrow;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.connector.ExternalStreamConnectorSplit;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
import org.boostscale.velox4j.connector.ExternalStreams;
import org.boostscale.velox4j.data.BaseVectorTests;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.iterator.CloseableIterator;
import org.boostscale.velox4j.iterator.UpIterators;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.test.SampleQueryTests;
import org.boostscale.velox4j.test.Velox4jTests;
import org.boostscale.velox4j.type.RowType;

/**
 * Tests that shared memory pools in MemoryManager work correctly across multiple sequential and
 * concurrent sessions.
 *
 * <p>Memory pools are intentionally shared by name (e.g. "Query Serde Memory Pool") across calls to
 * avoid creation overhead. Thread safety is ensured by {@code std::lock_guard} in {@code
 * getVeloxPool()} and {@code getArrowPool()}.
 */
public class MemoryPoolNameCollisionTest {

  private static BytesAllocationListener allocationListener;
  private static MemoryManager memoryManager;

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

  /**
   * Run multiple queries sequentially on the same MemoryManager using different Sessions. Verifies
   * that shared memory pools are correctly reused across sessions.
   */
  @Test
  public void testMultipleQueriesOnSameMemoryManager() throws Exception {
    for (int i = 0; i < 5; i++) {
      Session session = Velox4j.newSession(memoryManager);
      try {
        runSimpleQuery(session);
      } finally {
        session.close();
      }
    }
  }

  /** Run multiple queries sequentially on the same Session. Tests pool reuse within a session. */
  @Test
  public void testMultipleQueriesOnSameSession() throws Exception {
    Session session = Velox4j.newSession(memoryManager);
    try {
      for (int i = 0; i < 5; i++) {
        runSimpleQuery(session);
      }
    } finally {
      session.close();
    }
  }

  /**
   * Run multiple queries concurrently on the same MemoryManager from different threads. Tests
   * thread safety of pool creation.
   */
  @Test
  public void testConcurrentQueriesOnSameMemoryManager() throws InterruptedException {
    final int numThreads = 4;
    Thread[] threads = new Thread[numThreads];
    final Throwable[] errors = new Throwable[numThreads];

    for (int t = 0; t < numThreads; t++) {
      final int threadIdx = t;
      threads[t] =
          new Thread(
              () -> {
                Session session = Velox4j.newSession(memoryManager);
                try {
                  for (int i = 0; i < 3; i++) {
                    runSimpleQuery(session);
                  }
                } catch (Throwable e) {
                  errors[threadIdx] = e;
                } finally {
                  session.close();
                }
              },
              "pool-collision-test-" + t);
      threads[t].start();
    }

    for (Thread thread : threads) {
      thread.join(30_000);
    }

    for (int t = 0; t < numThreads; t++) {
      if (errors[t] != null) {
        Assert.fail("Thread " + t + " failed: " + errors[t].getMessage());
      }
    }
  }

  /** Run multiple Arrow import operations on the same MemoryManager across different sessions. */
  @Test
  public void testMultipleArrowImportsOnSameMemoryManager() {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try {
      for (int i = 0; i < 5; i++) {
        Session session = Velox4j.newSession(memoryManager);
        try {
          runArrowRoundTrip(session, allocator);
        } finally {
          session.close();
        }
      }
    } finally {
      allocator.close();
    }
  }

  private void runSimpleQuery(Session session) throws Exception {
    RowType schema = SampleQueryTests.getSchema();
    ExternalStreams.BlockingQueue queue = session.externalStreamOps().newBlockingQueue();
    TableScanNode scanNode =
        new TableScanNode(
            "scan-1",
            schema,
            new ExternalStreamTableHandle("connector-external-stream"),
            ImmutableList.of());
    Query query = new Query(scanNode, Config.empty(), ConnectorConfig.empty());
    SerialTask task = session.queryOps().execute(query);
    task.addSplit(
        scanNode.getId(),
        new ExternalStreamConnectorSplit("connector-external-stream", queue.id()));
    task.noMoreSplits(scanNode.getId());

    RowVector rv = BaseVectorTests.newSampleRowVector(session);
    queue.put(rv);
    queue.noMoreInput();

    CloseableIterator<RowVector> iter = UpIterators.asJavaIterator(task);
    int count = 0;
    while (iter.hasNext()) {
      RowVector result = iter.next();
      Assert.assertNotNull(result);
      count++;
    }
    iter.close();
    Assert.assertTrue("Expected at least 1 result batch", count > 0);
  }

  private void runArrowRoundTrip(Session session, BufferAllocator allocator) {
    RowVector rv = BaseVectorTests.newSampleRowVector(session);

    // Velox → Arrow (uses toArrowVectorSchemaRoot internally)
    VectorSchemaRoot arrowRoot = Arrow.toArrowVectorSchemaRoot(allocator, rv);
    Assert.assertNotNull(arrowRoot);
    Assert.assertTrue(arrowRoot.getRowCount() > 0);

    // Arrow → Velox (uses "Arrow Import Memory Pool" path)
    RowVector roundTripped = session.arrowOps().fromArrowVectorSchemaRoot(allocator, arrowRoot);
    Assert.assertNotNull(roundTripped);
    Assert.assertEquals(rv.getSize(), roundTripped.getSize());

    arrowRoot.close();
  }
}
