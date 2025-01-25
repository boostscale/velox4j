package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.lifecycle.CppObject;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

/**
 * The higher-level JNI-based API than {@link JniWrapper}. The API hides C++ pointers from
 * developers with providing objective representations in Java to caller.
 */
public final class JniApi implements CppObject {
  public static JniApi create() {
    return new JniApi();
  }

  private final JniWrapper jni = JniWrapper.create();

  private JniApi() {
  }

  public UpIterator executeQuery(String jsonQuery) {
    return new UpIterator(this, jni.executeQuery(jsonQuery));
  }

  public void releaseCppObject(CppObject obj) {
    jni.releaseCppObject(obj.id());
  }

  public boolean upIteratorHasNext(UpIterator itr) {
    return jni.upIteratorHasNext(itr.id());
  }

  public RowVector upIteratorNext(UpIterator itr) {
    return new RowVector(this, jni.upIteratorNext(itr.id()));
  }

  public void baseVectorExportToArrow(BaseVector vector, ArrowSchema schema, ArrowArray array) {
    jni.baseVectorExportToArrow(vector.id(), schema.memoryAddress(), array.memoryAddress());
  }

  public String deserializeAndSerialize(String json) {
    return jni.deserializeAndSerialize(json);
  }

  @Override
  public JniApi jniApi() {
    return this;
  }

  @Override
  public long id() {
    return jni.sessionId();
  }
}
