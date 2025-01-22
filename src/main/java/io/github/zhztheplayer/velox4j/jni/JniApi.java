package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.lifecycle.CppObject;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

/**
 * The higher-level JNI-based API than {@link JniWrapper}. The API hides C++ pointers from
 * developers with providing objective representations in Java to caller.
 */
public final class JniApi {
  public static UpIterator executePlan(String jsonPlan) {
    return new UpIterator(JniWrapper.executePlan(jsonPlan));
  }

  public static void closeCppObject(CppObject obj) {
    JniWrapper.closeCppObject(obj.address());
  }

  public static boolean upIteratorHasNext(UpIterator itr) {
    return JniWrapper.upIteratorHasNext(itr.address());
  }

  public static RowVector upIteratorNext(UpIterator itr) {
    return new RowVector(JniWrapper.upIteratorNext(itr.address()));
  }

  public static void rowVectorExportToArrow(RowVector rowVector, ArrowSchema schema, ArrowArray array) {
    JniWrapper.rowVectorExportToArrow(rowVector.address(), schema.memoryAddress(), array.memoryAddress());
  }
}
