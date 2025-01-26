package io.github.zhztheplayer.velox4j.bean;

import io.github.zhztheplayer.velox4j.connector.HiveConnectorSplit;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConcatTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.DereferenceTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.InputTypedExpr;
import io.github.zhztheplayer.velox4j.expression.LambdaTypedExpr;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistryFactory;
import io.github.zhztheplayer.velox4j.connector.HiveColumnHandle;
import io.github.zhztheplayer.velox4j.type.*;

public final class VeloxBeans {
  private static final SerdeRegistry ROOT_REGISTRY = SerdeRegistryFactory.get().key("name");

  private VeloxBeans() {

  }

  public static void registerAll() {
    registerTypes();
    registerExprs();
    registerConnectors();
  }

  private static void registerTypes() {
    final SerdeRegistry typeRegistry = ROOT_REGISTRY
        .registerFactory("Type")
        .key("type");
    typeRegistry.registerClass("BOOLEAN", BooleanType.class);
    typeRegistry.registerClass("TINYINT", TinyIntType.class);
    typeRegistry.registerClass("SMALLINT", SmallIntType.class);
    typeRegistry.registerClass("INTEGER", IntegerType.class);
    typeRegistry.registerClass("BIGINT", BigIntType.class);
    typeRegistry.registerClass("HUGEINT", HugeIntType.class);
    typeRegistry.registerClass("REAL", RealType.class);
    typeRegistry.registerClass("DOUBLE", DoubleType.class);
    typeRegistry.registerClass("VARCHAR", VarcharType.class);
    typeRegistry.registerClass("VARBINARY", VarbinaryType.class);
    typeRegistry.registerClass("TIMESTAMP", TimestampType.class);
    typeRegistry.registerClass("ARRAY", ArrayType.class);
    typeRegistry.registerClass("MAP", MapType.class);
    typeRegistry.registerClass("ROW", RowType.class);
    typeRegistry.registerClass("FUNCTION", FunctionType.class);
    typeRegistry.registerClass("UNKNOWN", UnknownType.class);
    typeRegistry.registerClass("OPAQUE", OpaqueType.class);
    typeRegistry.registerClass("DECIMAL", DecimalType.class);
    ROOT_REGISTRY.registerFactory("IntervalDayTimeType")
        .key("type")
        .registerClass("INTERVAL DAY TO SECOND", IntervalDayTimeType.class);
    ROOT_REGISTRY.registerFactory("IntervalYearMonthType")
        .key("type")
        .registerClass("INTERVAL YEAR TO MONTH", IntervalYearMonthType.class);
    ROOT_REGISTRY.registerFactory("DateType")
        .key("type")
        .registerClass("DATE", DateType.class);
  }

  private static void registerExprs() {
    ROOT_REGISTRY.registerClass("CallTypedExpr", CallTypedExpr.class);
    ROOT_REGISTRY.registerClass("CastTypedExpr", CastTypedExpr.class);
    ROOT_REGISTRY.registerClass("ConcatTypedExpr", ConcatTypedExpr.class);
    ROOT_REGISTRY.registerClass("ConstantTypedExpr", ConstantTypedExpr.class);
    ROOT_REGISTRY.registerClass("DereferenceTypedExpr", DereferenceTypedExpr.class);
    ROOT_REGISTRY.registerClass("FieldAccessTypedExpr", FieldAccessTypedExpr.class);
    ROOT_REGISTRY.registerClass("InputTypedExpr", InputTypedExpr.class);
    ROOT_REGISTRY.registerClass("LambdaTypedExpr", LambdaTypedExpr.class);
  }

  private static void registerConnectors() {
    ROOT_REGISTRY.registerClass("HiveColumnHandle", HiveColumnHandle.class);
    ROOT_REGISTRY.registerClass("HiveConnectorSplit", HiveConnectorSplit.class);
  }
}
