package io.github.zhztheplayer.velox4j.bean;

import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistryFactory;
import io.github.zhztheplayer.velox4j.type.*;

import java.math.BigInteger;

public final class VeloxBeans {
  private VeloxBeans() {

  }

  public static void registerAll() {
    final SerdeRegistry typeRegistry = SerdeRegistryFactory.get().key("name")
        .registerFactory("Type")
        .key("type");

    typeRegistry.registerClass("BOOLEAN", BooleanType.class);
    typeRegistry.registerClass("TINYINT", TinyIntType.class);
    typeRegistry.registerClass("SMALLINT", SmallIntType.class);
    typeRegistry.registerClass("INTEGER", Integer.class);
    typeRegistry.registerClass("BIGINT", BigInteger.class);
    typeRegistry.registerClass("HUGEINT", HugeIntType.class);
    typeRegistry.registerClass("REAL", RealType.class);
    typeRegistry.registerClass("DOUBLE", DoubleType.class);
    typeRegistry.registerClass("VARCHAR", VarcharType.class);
    typeRegistry.registerClass("VARBINARY", VarbinaryType.class);
    typeRegistry.registerClass("TIMESTAMP", TimestampType.class);
    typeRegistry.registerClass("ARRAY", ArrayType.class);
  }
}
