package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.data.BaseVectors;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.data.SelectivityVectors;
import io.github.zhztheplayer.velox4j.eval.Evaluations;
import io.github.zhztheplayer.velox4j.query.Queries;
import io.github.zhztheplayer.velox4j.serializable.ISerializables;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.variant.Variants;
import io.github.zhztheplayer.velox4j.write.TableWriteTraits;

public class LocalSession implements Session {
  private final long id;

  LocalSession(long id) {
    this.id = id;
  }

  private JniApi jniApi() {
    final JniWrapper jniWrapper = new JniWrapper(this.id);
    return new JniApi(jniWrapper);
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Evaluations evaluationOps() {
    return new Evaluations(jniApi());
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

  @Override
  public TableWriteTraits tableWriteTraitsOps() {
    return new TableWriteTraits(jniApi());
  }

  @Override
  public ISerializables iSerializableOps() {
    return new ISerializables(jniApi());
  }

  @Override
  public Variants variantOps() {
    return new Variants(jniApi());
  }
}
