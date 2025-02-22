package io.github.zhztheplayer.velox4j.write;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.type.RowType;

public class TableWriteTraits {
  private final JniApi jniApi;

  public TableWriteTraits(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public static RowType outputType() {
    return StaticJniApi.get().tableWriteTraitsOutputType();
  }

  public RowType outputType(AggregationNode aggregationNode) {
    return jniApi.tableWriteTraitsOutputTypeWithAggregationNode(aggregationNode);
  }
}
