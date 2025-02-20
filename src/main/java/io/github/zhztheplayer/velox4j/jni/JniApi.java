package io.github.zhztheplayer.velox4j.jni;

import com.google.common.annotations.VisibleForTesting;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.VectorEncoding;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.Variant;
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
  static JniApi create(Session session) {
    return new JniApi(JniWrapper.create(session.id()));
  }

  private final JniWrapper jni;

  private JniApi(JniWrapper jni) {
    this.jni = jni;
  }

  public UpIterator executeQuery(String queryJson) {
    return new UpIterator(this, jni.executeQuery(queryJson));
  }

  public RowVector upIteratorNext(UpIterator itr) {
    return rowVectorWrap(jni.upIteratorNext(itr.id()));
  }

  public ExternalStream newExternalStream(DownIterator itr) {
    return new ExternalStream(jni.newExternalStream(itr));
  }

  private BaseVector baseVectorWrap(long id) {
    // TODO Add JNI API `isRowVector` for performance.
    final VectorEncoding encoding = VectorEncoding.valueOf(
        StaticJniWrapper.get().baseVectorGetEncoding(id));
    if (encoding == VectorEncoding.ROW) {
      return new RowVector(this, id);
    }
    return new BaseVector(this, id);
  }

  private RowVector rowVectorWrap(long id) {
    final BaseVector vector = baseVectorWrap(id);
    if (vector instanceof RowVector) {
      return ((RowVector) vector);
    }
    throw new VeloxException("Expected RowVector, got " + vector.getClass().getName());
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

  public RowVector baseVectorAsRowVector(BaseVector vector) {
    return rowVectorWrap(jni.baseVectorNewRef(vector.id()));
  }

  @VisibleForTesting
  public String deserializeAndSerialize(String json) {
    return jni.deserializeAndSerialize(json);
  }

  @VisibleForTesting
  public UpIterator createUpIteratorWithExternalStream(ExternalStream es) {
    return new UpIterator(this, jni.createUpIteratorWithExternalStream(es.id()));
  }
}
