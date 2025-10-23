/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.boostscale.velox4j.jni;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

import org.boostscale.velox4j.connector.ExternalStream;
import org.boostscale.velox4j.connector.ExternalStreams;
import org.boostscale.velox4j.data.*;
import org.boostscale.velox4j.eval.Evaluation;
import org.boostscale.velox4j.eval.Evaluator;
import org.boostscale.velox4j.iterator.DownIterator;
import org.boostscale.velox4j.iterator.GenericUpIterator;
import org.boostscale.velox4j.iterator.UpIterator;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.QueryExecutor;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.serde.Serde;
import org.boostscale.velox4j.serializable.ISerializable;
import org.boostscale.velox4j.serializable.ISerializableCo;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.variant.Variant;
import org.boostscale.velox4j.variant.VariantCo;
import org.boostscale.velox4j.write.ColumnStatsSpec;

/**
 * The higher-level JNI-based API over {@link JniWrapper}. The API hides details like native
 * pointers and serialized data from developers, instead provides objective forms of the required
 * functionalities.
 */
public final class JniApi {
  private final JniWrapper jni;

  JniApi(JniWrapper jni) {
    this.jni = jni;
  }

  public Evaluator createEvaluator(Evaluation evaluation) {
    final String evalJson = Serde.toPrettyJson(evaluation);
    return new Evaluator(this, jni.createEvaluator(evalJson));
  }

  public BaseVector evaluatorEval(Evaluator evaluator, SelectivityVector sv, RowVector input) {
    return baseVectorWrap(jni.evaluatorEval(evaluator.id(), sv.id(), input.id()));
  }

  public QueryExecutor createQueryExecutor(Query query) {
    final String queryJson = Serde.toPrettyJson(query);
    return new QueryExecutor(this, jni.createQueryExecutor(queryJson));
  }

  @VisibleForTesting
  QueryExecutor createQueryExecutor(String queryJson) {
    return new QueryExecutor(this, jni.createQueryExecutor(queryJson));
  }

  public SerialTask queryExecutorExecute(QueryExecutor executor) {
    return new SerialTask(this, jni.queryExecutorExecute(executor.id()));
  }

  public RowVector upIteratorGet(UpIterator itr) {
    return baseVectorWrap(jni.upIteratorGet(itr.id())).asRowVector();
  }

  public ExternalStream createExternalStreamFromDownIterator(DownIterator itr) {
    return new ExternalStreams.GenericExternalStream(jni.createExternalStreamFromDownIterator(itr));
  }

  public ExternalStreams.BlockingQueue createBlockingQueue() {
    return new ExternalStreams.BlockingQueue(jni.createBlockingQueue());
  }

  public void typeToArrow(Type type, ArrowSchema schema) {
    final String typeJson = Serde.toJson(type);
    jni.typeToArrow(typeJson, schema.memoryAddress());
  }

  public BaseVector createEmptyBaseVector(Type type) {
    final String typeJson = Serde.toJson(type);
    return baseVectorWrap(jni.createEmptyBaseVector(typeJson));
  }

  public BaseVector arrowToBaseVector(ArrowSchema schema, ArrowArray array) {
    try {
      return baseVectorWrap(jni.arrowToBaseVector(schema.memoryAddress(), array.memoryAddress()));
    } finally {
      schema.close();
      array.close();
    }
  }

  public List<BaseVector> baseVectorDeserialize(String serialized) {
    return Arrays.stream(jni.baseVectorDeserialize(serialized))
        .mapToObj(this::baseVectorWrap)
        .collect(Collectors.toList());
  }

  public BaseVector baseVectorWrapInConstant(BaseVector vector, int length, int index) {
    return baseVectorWrap(jni.baseVectorWrapInConstant(vector.id(), length, index));
  }

  public BaseVector baseVectorSlice(BaseVector vector, int offset, int length) {
    return baseVectorWrap(jni.baseVectorSlice(vector.id(), offset, length));
  }

  public List<RowVector> rowVectorPartitionByKeys(RowVector vector, List<Integer> keyChannels) {
    final int[] keyChannelArray = keyChannels.stream().mapToInt(i -> i).toArray();
    final long[] vids = jni.rowVectorPartitionByKeys(vector.id(), keyChannelArray);
    return Arrays.stream(vids)
        .mapToObj(this::baseVectorWrap)
        .map(BaseVector::asRowVector)
        .collect(Collectors.toList());
  }

  public BaseVector flattenVector(BaseVector vector) {
    return baseVectorWrap(jni.baseVectorFlatten(vector.id()));
  }

  public SelectivityVector createSelectivityVector(int length) {
    return new SelectivityVector(jni.createSelectivityVector(length));
  }

  public RowType tableWriteTraitsOutputTypeFromColumnStatsSpec(ColumnStatsSpec columnStatsSpec) {
    final String columnStatsSpecJson = Serde.toJson(columnStatsSpec);
    final String typeJson = jni.tableWriteTraitsOutputTypeFromColumnStatsSpec(columnStatsSpecJson);
    final RowType type = Serde.fromJson(typeJson, RowType.class);
    return type;
  }

  public ISerializableCo iSerializableAsCpp(ISerializable iSerializable) {
    final String json = Serde.toPrettyJson(iSerializable);
    return new ISerializableCo(jni.iSerializableAsCpp(json));
  }

  public VariantCo variantAsCpp(Variant variant) {
    final String json = Serde.toPrettyJson(variant);
    return new VariantCo(jni.variantAsCpp(json));
  }

  public BaseVector variantToVector(Type type, Variant variant) {
    final String typeJson = Serde.toJson(type);
    final String variantJson = Serde.toPrettyJson(variant);
    return baseVectorWrap(jni.variantToVector(typeJson, variantJson));
  }

  @VisibleForTesting
  public UpIterator createUpIteratorWithExternalStream(ExternalStream es) {
    return new GenericUpIterator(this, jni.createUpIteratorWithExternalStream(es.id()));
  }

  private BaseVector baseVectorWrap(long id) {
    final VectorEncoding encoding =
        VectorEncoding.valueOf(StaticJniWrapper.get().baseVectorGetEncoding(id));
    return BaseVector.wrap(this, id, encoding);
  }
}
