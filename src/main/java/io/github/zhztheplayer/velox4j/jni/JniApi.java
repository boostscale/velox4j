package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.VectorEncoding;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.serde.Serde;
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
public final class JniApi implements AutoCloseable {
  public static JniApi create(MemoryManager memoryManager) {
    final Session session = StaticJniApi.get().createSession(memoryManager);
    return new JniApi(session, JniWrapper.create(session));
  }

  private final Session session;
  private final JniWrapper jni;

  private JniApi(Session session, JniWrapper jni) {
    this.session = session;
    this.jni = jni;
  }

  public UpIterator executeQuery(String queryJson) {
    return new UpIterator(this, jni.executeQuery(queryJson));
  }

  public boolean upIteratorHasNext(UpIterator itr) {
    return jni.upIteratorHasNext(itr.id());
  }

  public RowVector upIteratorNext(UpIterator itr) {
    return rowVectorWrap(jni.upIteratorNext(itr.id()));
  }

  public ExternalStream downIteratorAsExternalStream(DownIterator itr) {
    return new ExternalStream(jni.downIteratorAsExternalStream(itr));
  }

  public Type variantInferType(Variant variant) {
    final String variantJson = Serde.toJson(variant);
    final String typeJson = jni.variantInferType(variantJson);
    return Serde.fromJson(typeJson, Type.class);
  }

  private BaseVector baseVectorWrap(long id) {
    // TODO Add JNI API `isRowVector` for performance.
    final VectorEncoding encoding = VectorEncoding.valueOf(jni.baseVectorGetEncoding(id));
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

  public void baseVectorToArrow(BaseVector vector, ArrowSchema schema, ArrowArray array) {
    jni.baseVectorToArrow(vector.id(), schema.memoryAddress(), array.memoryAddress());
  }

  public String baseVectorSerialize(List<? extends BaseVector> vector) {
    return jni.baseVectorSerialize(vector.stream().mapToLong(BaseVector::id).toArray());
  }

  public List<BaseVector> baseVectorDeserialize(String serialized) {
    return Arrays.stream(jni.baseVectorDeserialize(serialized))
        .mapToObj(this::baseVectorWrap)
        .collect(Collectors.toList());
  }

  public Type baseVectorGetType(BaseVector vector) {
    String typeJson = jni.baseVectorGetType(vector.id());
    return Serde.fromJson(typeJson, Type.class);
  }

  public BaseVector baseVectorWrapInConstant(BaseVector vector, int length, int index) {
    return baseVectorWrap(jni.baseVectorWrapInConstant(vector.id(), length, index));
  }

  public VectorEncoding baseVectorGetEncoding(BaseVector vector) {
    return VectorEncoding.valueOf(jni.baseVectorGetEncoding(vector.id()));
  }

  public RowVector baseVectorAsRowVector(BaseVector vector) {
    return rowVectorWrap(jni.baseVectorNewRef(vector.id()));
  }

  // For tests.
  public String deserializeAndSerialize(String json) {
    return jni.deserializeAndSerialize(json);
  }

  // For tests.
  public String deserializeAndSerializeVariant(String json) {
    return jni.deserializeAndSerializeVariant(json);
  }

  // For tests.
  public UpIterator createUpIteratorWithExternalStream(ExternalStream es) {
    return new UpIterator(this, jni.createUpIteratorWithExternalStream(es.id()));
  }

  @Override
  public void close() {
    session.close();
  }
}
