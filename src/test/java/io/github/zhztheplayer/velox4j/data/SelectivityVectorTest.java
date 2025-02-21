package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SelectivityVectorTest {
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
  public void testIsValid() {
    final Session session = Velox4j.newSession(memoryManager);
    final int length = 10;
    final SelectivityVector sv = session.selectivityVectorOps().create(length);
    for (int i = 0; i < length; i++) {
      Assert.assertTrue(sv.isValid(i));
    }
    session.close();
  }
}
