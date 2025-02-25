package io.github.zhztheplayer.velox4j.session;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.data.BaseVectors;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.data.SelectivityVectors;
import io.github.zhztheplayer.velox4j.eval.Evaluations;
import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.query.Queries;
import io.github.zhztheplayer.velox4j.serializable.ISerializables;
import io.github.zhztheplayer.velox4j.variant.Variants;
import io.github.zhztheplayer.velox4j.write.TableWriteTraits;

public interface Session extends CppObject {
  Evaluations evaluationOps();

  Queries queryOps();

  ExternalStreams externalStreamOps();

  BaseVectors baseVectorOps();

  RowVectors rowVectorOps();

  SelectivityVectors selectivityVectorOps();

  Arrow arrowOps();

  TableWriteTraits tableWriteTraitsOps();

  ISerializables iSerializableOps();

  Variants variantOps();
}
