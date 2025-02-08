package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.VarCharType;

import java.io.File;
import java.util.List;

public final class TpchTests {
  private static final String DATA_DIRECTORY = "data/tpch-sf0.1/nation";

  public enum Table {
    NATION("nation.parquet", new RowType(List.of(
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment"
    ), List.of(
        new BigIntType(),
        new VarCharType(),
        new BigIntType(),
        new VarCharType()
    )));

    private final RowType schema;
    private final File file;

    Table(String fileName, RowType schema) {
      this.schema = schema;
      this.file = Resources.copyResourceToTmp(String.format("%s/%s", DATA_DIRECTORY, fileName));
    }

    public RowType schema() {
      return schema;
    }

    public File file() {
      return file;
    }
  }
}
