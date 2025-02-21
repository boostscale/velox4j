package io.github.zhztheplayer.velox4j.jni;

import com.google.common.annotations.VisibleForTesting;
import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.SelectivityVector;
import io.github.zhztheplayer.velox4j.data.VectorEncoding;
import io.github.zhztheplayer.velox4j.eval.Evaluator;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The higher-level JNI-based API over {@link JniWrapper}. The API hides
 * details like native pointers and serialized data from developers, instead
 * provides objective forms of the required functionalities.
 */
public final class JniApi {
  private final JniWrapper jni;

  JniApi(JniWrapper jni) {
    this.jni = jni;
  }

  public Evaluator createEvaluator(String evalJson) {
    return new Evaluator(this, jni.createEvaluator(evalJson));
  }

  public BaseVector evaluatorEval(Evaluator evaluator, SelectivityVector sv, RowVector input) {
    return baseVectorWrap(jni.evaluatorEval(evaluator.id(), sv.id(), input.id()));
  }

  public UpIterator executeQuery(String queryJson) {
    return new UpIterator(this, jni.executeQuery(queryJson));
  }

  public RowVector upIteratorNext(UpIterator itr) {
    return baseVectorWrap(jni.upIteratorNext(itr.id())).asRowVector();
  }

  public ExternalStream newExternalStream(DownIterator itr) {
    return new ExternalStream(jni.newExternalStream(itr));
  }

  public BaseVector arrowToBaseVector(ArrowSchema schema, ArrowArray array) {
    return baseVectorWrap(jni.arrowToBaseVector(schema.memoryAddress(), array.memoryAddress()));
  }

  public List<BaseVector> baseVectorDeserialize(String serialized) {
    return Arrays.stream(jni.baseVectorDeserialize(serialized))
        .mapToObj(this::baseVectorWrap)
        .collect(Collectors.toList());
  }

  public BaseVector baseVectorWrapInConstant(BaseVector vector, int length, int index) {
    return baseVectorWrap(jni.baseVectorWrapInConstant(vector.id(), length, index));
  }

  public SelectivityVector createSelectivityVector(int length) {
    return new SelectivityVector(jni.createSelectivityVector(length));
  }

  @VisibleForTesting
  public String deserializeAndSerialize(String json) {
    return jni.deserializeAndSerialize(json);
  }

  @VisibleForTesting
  public UpIterator createUpIteratorWithExternalStream(ExternalStream es) {
    return new UpIterator(this, jni.createUpIteratorWithExternalStream(es.id()));
  }

  private BaseVector baseVectorWrap(long id) {
    final VectorEncoding encoding = VectorEncoding.valueOf(
        StaticJniWrapper.get().baseVectorGetEncoding(id));
    return BaseVector.wrap(this, id, encoding);
  }
}
