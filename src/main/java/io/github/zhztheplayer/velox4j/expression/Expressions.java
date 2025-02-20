package io.github.zhztheplayer.velox4j.expression;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.serde.Serde;

public class Expressions {
  private final JniApi jniApi;

  public Expressions(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public Evaluator createEvaluator(Expression expression) {
    return jniApi.createEvaluator(Serde.toPrettyJson(expression));
  }
}
