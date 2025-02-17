package io.github.zhztheplayer.velox4j.serializable;

import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.connector.HiveColumnHandle;
import io.github.zhztheplayer.velox4j.connector.HiveConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.HiveTableHandle;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConcatTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.DereferenceTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.InputTypedExpr;
import io.github.zhztheplayer.velox4j.expression.LambdaTypedExpr;
import io.github.zhztheplayer.velox4j.filter.AlwaysTrue;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.FilterNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.plan.ValuesNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistryFactory;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.DateType;
import io.github.zhztheplayer.velox4j.type.DecimalType;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.FunctionType;
import io.github.zhztheplayer.velox4j.type.HugeIntType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.IntervalDayTimeType;
import io.github.zhztheplayer.velox4j.type.IntervalYearMonthType;
import io.github.zhztheplayer.velox4j.type.MapType;
import io.github.zhztheplayer.velox4j.type.OpaqueType;
import io.github.zhztheplayer.velox4j.type.RealType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.SmallIntType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.TinyIntType;
import io.github.zhztheplayer.velox4j.type.UnknownType;
import io.github.zhztheplayer.velox4j.type.VarbinaryType;
import io.github.zhztheplayer.velox4j.type.VarCharType;

public final class VeloxSerializables {
  private static final SerdeRegistry NAME_REGISTRY = SerdeRegistryFactory
      .createForBaseClass(VeloxSerializable.class).key("name");

  private VeloxSerializables() {

  }

  public static void registerAll() {
    Serde.registerBaseClass(VeloxSerializable.class);
    registerTypes();
    registerExprs();
    registerConnectors();
    registerFilters();
    registerPlanNodes();
    retisterConfig();
    registerQuery();
  }

  private static void registerTypes() {
    final SerdeRegistry typeRegistry = NAME_REGISTRY
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
    typeRegistry.registerClass("VARCHAR", VarCharType.class);
    typeRegistry.registerClass("VARBINARY", VarbinaryType.class);
    typeRegistry.registerClass("TIMESTAMP", TimestampType.class);
    typeRegistry.registerClass("ARRAY", ArrayType.class);
    typeRegistry.registerClass("MAP", MapType.class);
    typeRegistry.registerClass("ROW", RowType.class);
    typeRegistry.registerClass("FUNCTION", FunctionType.class);
    typeRegistry.registerClass("UNKNOWN", UnknownType.class);
    typeRegistry.registerClass("OPAQUE", OpaqueType.class);
    typeRegistry.registerClass("DECIMAL", DecimalType.class);
    NAME_REGISTRY.registerFactory("IntervalDayTimeType")
        .key("type")
        .registerClass("INTERVAL DAY TO SECOND", IntervalDayTimeType.class);
    NAME_REGISTRY.registerFactory("IntervalYearMonthType")
        .key("type")
        .registerClass("INTERVAL YEAR TO MONTH", IntervalYearMonthType.class);
    NAME_REGISTRY.registerFactory("DateType")
        .key("type")
        .registerClass("DATE", DateType.class);
  }

  private static void registerExprs() {
    NAME_REGISTRY.registerClass("CallTypedExpr", CallTypedExpr.class);
    NAME_REGISTRY.registerClass("CastTypedExpr", CastTypedExpr.class);
    NAME_REGISTRY.registerClass("ConcatTypedExpr", ConcatTypedExpr.class);
    NAME_REGISTRY.registerClass("ConstantTypedExpr", ConstantTypedExpr.class);
    NAME_REGISTRY.registerClass("DereferenceTypedExpr", DereferenceTypedExpr.class);
    NAME_REGISTRY.registerClass("FieldAccessTypedExpr", FieldAccessTypedExpr.class);
    NAME_REGISTRY.registerClass("InputTypedExpr", InputTypedExpr.class);
    NAME_REGISTRY.registerClass("LambdaTypedExpr", LambdaTypedExpr.class);
  }

  private static void registerConnectors() {
    NAME_REGISTRY.registerClass("HiveColumnHandle", HiveColumnHandle.class);
    NAME_REGISTRY.registerClass("HiveConnectorSplit", HiveConnectorSplit.class);
    NAME_REGISTRY.registerClass("HiveTableHandle", HiveTableHandle.class);
    NAME_REGISTRY.registerClass("ExternalStreamConnectorSplit", ExternalStreamConnectorSplit.class);
    NAME_REGISTRY.registerClass("ExternalStreamTableHandle", ExternalStreamTableHandle.class);
  }

  private static void registerFilters() {
    NAME_REGISTRY.registerClass("AlwaysTrue", AlwaysTrue.class);
  }

  private static void registerPlanNodes() {
    NAME_REGISTRY.registerClass("ValuesNode", ValuesNode.class);
    NAME_REGISTRY.registerClass("TableScanNode", TableScanNode.class);
    NAME_REGISTRY.registerClass("AggregationNode", AggregationNode.class);
    NAME_REGISTRY.registerClass("ProjectNode", ProjectNode.class);
    NAME_REGISTRY.registerClass("FilterNode", FilterNode.class);
  }

  private static void retisterConfig() {
    NAME_REGISTRY.registerClass("Velox4jConfig", Config.class);
    NAME_REGISTRY.registerClass("Velox4jConnectorConfig", ConnectorConfig.class);
  }

  private static void registerQuery() {
    NAME_REGISTRY.registerClass("Velox4jQuery", Query.class);
  }
}
