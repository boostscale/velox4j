package io.github.zhztheplayer.velox4j.jni;

import com.google.common.annotations.VisibleForTesting;
import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.Variant;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

import java.util.List;

public class LocalSession implements Session {
  private final long id;
  private final JniApi jni;

  private LocalSession(long id) {
    this.id = id;
    this.jni = JniApi.create(this);
  }

  static LocalSession create(long id) {
    return new LocalSession(id);
  }

  @Override
  public long id() {
    return id;
  }

  @VisibleForTesting
  JniApi jniApi() {
    return jni;
  }

  @Override
  public UpIterator executeQuery(Query query) {
    return jni.executeQuery(Serde.toPrettyJson(query));
  }

  @Override
  public ExternalStream newExternalStream(DownIterator itr) {
    return jni.newExternalStream(itr);
  }

  @Override
  public String baseVectorSerialize(List<? extends BaseVector> vector) {
    return jni.baseVectorSerialize(vector);
  }

  @Override
  public List<BaseVector> baseVectorDeserialize(String serialized) {
    return jni.baseVectorDeserialize(serialized);
  }

  @Override
  public BaseVector arrowToBaseVector(ArrowSchema schema, ArrowArray array) {
    return jni.arrowToBaseVector(schema, array);
  }

  @Override
  public Type variantInferType(Variant variant) {
    return jni.variantInferType(variant);
  }

  @Override
  public String deserializeAndSerialize(String json) {
    return jni.deserializeAndSerialize(json);
  }

  @Override
  public String deserializeAndSerializeVariant(String json) {
    return jni.deserializeAndSerializeVariant(json);
  }

  @Override
  public UpIterator createUpIteratorWithExternalStream(ExternalStream es) {
    return jni.createUpIteratorWithExternalStream(es);
  }
}
