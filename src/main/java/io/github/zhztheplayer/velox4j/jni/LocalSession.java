package io.github.zhztheplayer.velox4j.jni;

import com.google.common.annotations.VisibleForTesting;
import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.data.BaseVectors;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.data.SelectivityVectors;
import io.github.zhztheplayer.velox4j.expression.Expressions;
import io.github.zhztheplayer.velox4j.query.Queries;
import io.github.zhztheplayer.velox4j.session.Session;

public class LocalSession implements Session {
  private final long id;

  LocalSession(long id) {
    this.id = id;
  }

  private JniApi jniApi() {
    return JniApi.create(this);
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Expressions expressionOps() {
    return new Expressions(jniApi());
  }

  @Override
  public Queries queryOps() {
    return new Queries(jniApi());
  }

  @Override
  public ExternalStreams externalStreamOps() {
    return new ExternalStreams(jniApi());
  }

  @Override
  public BaseVectors baseVectorOps() {
    return new BaseVectors(jniApi());
  }

  @Override
  public RowVectors rowVectorOps() {
    return new RowVectors(jniApi());
  }

  @Override
  public SelectivityVectors selectivityVectorOps() {
    return new SelectivityVectors(jniApi());
  }

  @Override
  public Arrow arrowOps() {
    return new Arrow(jniApi());
  }
}
