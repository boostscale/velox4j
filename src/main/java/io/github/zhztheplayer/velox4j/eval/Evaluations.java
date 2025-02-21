package io.github.zhztheplayer.velox4j.eval;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.serde.Serde;

public class Evaluations {
  private final JniApi jniApi;

  public Evaluations(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public Evaluator createEvaluator(Evaluation evaluation) {
    return jniApi.createEvaluator(Serde.toPrettyJson(evaluation));
  }
}
