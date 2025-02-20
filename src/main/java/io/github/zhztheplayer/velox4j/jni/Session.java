package io.github.zhztheplayer.velox4j.jni;

import com.google.common.annotations.VisibleForTesting;
import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.Variant;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

import java.util.List;

public interface Session extends CppObject {
  static Session create(MemoryManager memoryManager) {
    return StaticJniApi.get().createSession(memoryManager);
  }

  UpIterator executeQuery(Query query);

  ExternalStream newExternalStream(DownIterator itr);

  String baseVectorSerialize(List<? extends BaseVector> vector);

  List<BaseVector> baseVectorDeserialize(String serialized);

  BaseVector arrowToBaseVector(ArrowSchema schema, ArrowArray array);

  Type variantInferType(Variant variant);

  @VisibleForTesting
  String deserializeAndSerialize(String json);

  @VisibleForTesting
  String deserializeAndSerializeVariant(String json);

  @VisibleForTesting
  UpIterator createUpIteratorWithExternalStream(ExternalStream es);
}
