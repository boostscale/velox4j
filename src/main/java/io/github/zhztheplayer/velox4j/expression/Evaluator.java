package io.github.zhztheplayer.velox4j.expression;

import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.SelectivityVector;
import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.JniApi;

public class Evaluator implements CppObject {
  private final JniApi jniApi;
  private final long id;

  public Evaluator(JniApi jniApi, long id) {
    this.jniApi = jniApi;
    this.id = id;
  }

  public JniApi jniApi() {
    return jniApi;
  }

  @Override
  public long id() {
    return id;
  }

  public BaseVector eval(SelectivityVector sv, RowVector input) {
    return jniApi.evaluatorEval(this, sv, input);
  }
}
