package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.split.FileFormat;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SplitSerdeTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testFileFormat() throws JsonProcessingException {
    final ObjectMapper jsonMapper = Serde.newVeloxJsonMapper();
    final FileFormat in = FileFormat.DWRF;
    final String json = jsonMapper.writeValueAsString(in);
    final FileFormat out = jsonMapper.readValue(json, FileFormat.class);
    Assert.assertEquals("1", json);
    Assert.assertEquals(in, out);
  }
}
