package io.github.zhztheplayer.velox4j.write;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.serde.SerdeTests;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.test.ResourceTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.RowType;
import org.junit.*;

public class TableWriteTraitsTest {
  private static MemoryManager memoryManager;
  private static Session session;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    memoryManager.close();
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
  public void testOutputType() {
    final RowType type = TableWriteTraits.outputType();
    Assert.assertEquals(
        ResourceTests.readResourceAsString("table-write-traits/output-type-1.json"),
        Serde.toPrettyJson(type)
    );
  }

  @Test
  public void testOutputTypeWithAggregationNode() {
    final RowType type = session.tableWriteTraitsOps().outputType(
        SerdeTests.newSampleAggregationNode("id-2", "id-1")
    );
    Assert.assertEquals(
        ResourceTests.readResourceAsString("table-write-traits/output-type-with-aggregation-node-1.json"),
        Serde.toPrettyJson(type)
    );
  }
}
