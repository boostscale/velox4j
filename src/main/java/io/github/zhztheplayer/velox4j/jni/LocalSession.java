package io.github.zhztheplayer.velox4j.jni;

import com.google.common.annotations.VisibleForTesting;
import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.data.BaseVectors;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.serde.Serde;

public class LocalSession implements Session {
  private final long id;
  private final JniApi jni;

  private LocalSession(long id) {
    this.id = id;
    this.jni = JniApi.create(id);
  }

  static LocalSession create(long id) {
    return new LocalSession(id);
  }

  @Override
  public long id() {
    return id;
  }

  @VisibleForTesting
  public JniApi jniApi() {
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
  public BaseVectors baseVectorOps() {
    return new BaseVectors(jni);
  }

  @Override
  public RowVectors rowVectorOps() {
    return new RowVectors(jni);
  }

  @Override
  public Arrow arrowOps() {
    return new Arrow(jni);
  }
}
