package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.DecimalType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.VarCharType;

import java.io.File;
import java.util.List;

public final class TpchTests {
  private static final String DATA_DIRECTORY = "data/tpch-sf0.1";

  public enum Table {
    REGION("region/region.parquet",
        new RowType(List.of(
            "r_regionkey",
            "r_name",
            "r_comment"
        ), List.of(
            new BigIntType(),
            new VarCharType(),
            new VarCharType()
        ))),

    CUSTOMER("customer/customer.parquet", new RowType(List.of(
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment"
    ), List.of(
        new BigIntType(),
        new VarCharType(),
        new VarCharType(),
        new BigIntType(),
        new VarCharType(),
        new DecimalType(12, 2),
        new VarCharType()
    ))),

    NATION("nation/nation.parquet", new RowType(List.of(
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
      this.file = ResourceTests.copyResourceToTmp(String.format("%s/%s", DATA_DIRECTORY, fileName));
    }

    public RowType schema() {
      return schema;
    }

    public File file() {
      return file;
    }
  }
}
