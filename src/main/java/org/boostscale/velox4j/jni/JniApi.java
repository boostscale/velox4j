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

import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.connector.ConnectorSplit;
import org.boostscale.velox4j.connector.ExternalStream;
import org.boostscale.velox4j.connector.ExternalStreams;
import org.boostscale.velox4j.data.*;
import org.boostscale.velox4j.eval.Evaluation;
import org.boostscale.velox4j.eval.Evaluator;
import org.boostscale.velox4j.iterator.DownIterator;
import org.boostscale.velox4j.iterator.GenericUpIterator;
import org.boostscale.velox4j.iterator.UpIterator;
import org.boostscale.velox4j.memory.AllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.partition.PartitionFunction;
import org.boostscale.velox4j.partition.PartitionFunctionSpec;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.QueryExecutor;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.query.SerialTaskStats;
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
  private static final Long INVALID_OBJECT_HANDLE = -1L;
  private final JniWrapper jni;

  JniApi(JniWrapper jni) {
    this.jni = jni;
  }

  // Global initialization.
  public static void initialize(Config globalConf) {
    JniWrapper.initialize(Serde.toPrettyJson(globalConf));
  }

  // Memory.
  public static MemoryManager createMemoryManager(AllocationListener listener) {
    return new MemoryManager(JniWrapper.createMemoryManager(listener));
  }

  // Lifecycle.
  public static LocalSession createSession(MemoryManager memoryManager) {
    return new LocalSession(JniWrapper.createSession(memoryManager.id()));
  }

  public static void releaseCppObject(CppObject obj) {
    JniWrapper.releaseCppObject(obj.id());
  }

  // Expression evaluation.
  public Evaluator createEvaluator(Evaluation evaluation) {
    final String evalJson = Serde.toPrettyJson(evaluation);
    return new Evaluator(this, jni.createEvaluator(evalJson));
  }

  public BaseVector evaluatorEval(Evaluator evaluator, SelectivityVector sv, RowVector input) {
    return baseVectorCreateById(jni.evaluatorEval(evaluator.id(), sv.id(), input.id()));
  }

  // Plan execution.
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

  // UpIterator.
  public static UpIterator.State upIteratorAdvance(UpIterator itr) {
    return UpIterator.State.get(JniWrapper.upIteratorAdvance(itr.id()));
  }

  public static void upIteratorWait(UpIterator itr) {
    JniWrapper.upIteratorWait(itr.id());
  }

  public RowVector upIteratorGet(UpIterator itr) {
    return baseVectorCreateById(jni.upIteratorGet(itr.id())).asRowVector();
  }

  // DownIterator.
  public static void blockingQueuePut(ExternalStreams.BlockingQueue queue, RowVector rowVector) {
    JniWrapper.blockingQueuePut(queue.id(), rowVector.id());
  }

  public static void blockingQueueNoMoreInput(ExternalStreams.BlockingQueue queue) {
    JniWrapper.blockingQueueNoMoreInput(queue.id());
  }

  public ExternalStream createExternalStreamFromDownIterator(DownIterator itr) {
    return new ExternalStreams.GenericExternalStream(jni.createExternalStreamFromDownIterator(itr));
  }

  public ExternalStreams.BlockingQueue createBlockingQueue() {
    return new ExternalStreams.BlockingQueue(jni.createBlockingQueue());
  }

  // SerialTask.
  public static void serialTaskAddSplit(
      SerialTask serialTask, String planNodeId, int groupId, ConnectorSplit split) {
    final String splitJson = Serde.toJson(split);
    JniWrapper.serialTaskAddSplit(serialTask.id(), planNodeId, groupId, splitJson);
  }

  public static void serialTaskNoMoreSplits(SerialTask serialTask, String planNodeId) {
    JniWrapper.serialTaskNoMoreSplits(serialTask.id(), planNodeId);
  }

  public static SerialTaskStats serialTaskCollectStats(SerialTask serialTask) {
    final String statsJson = JniWrapper.serialTaskCollectStats(serialTask.id());
    return SerialTaskStats.fromJson(statsJson);
  }

  // Variant.
  public static Type variantInferType(Variant variant) {
    final String variantJson = Serde.toJson(variant);
    final String typeJson = JniWrapper.variantInferType(variantJson);
    return Serde.fromJson(typeJson, Type.class);
  }

  public static Variant variantAsJava(VariantCo co) {
    final String json = JniWrapper.variantAsJava(co.id());
    return Serde.fromJson(json, Variant.class);
  }

  public VariantCo variantAsCpp(Variant variant) {
    final String json = Serde.toPrettyJson(variant);
    return new VariantCo(jni.variantAsCpp(json));
  }

  public BaseVector variantToVector(Type type, Variant variant) {
    final String typeJson = Serde.toJson(type);
    final String variantJson = Serde.toPrettyJson(variant);
    return baseVectorCreateById(jni.variantToVector(typeJson, variantJson));
  }

  // Type.
  public static Type arrowToRowType(ArrowSchema schema) {
    try {
      final String typeJson = JniWrapper.arrowToType(schema.memoryAddress());
      return Serde.fromJson(typeJson, Type.class);
    } finally {
      schema.close();
    }
  }

  public void typeToArrow(Type type, ArrowSchema schema) {
    final String typeJson = Serde.toJson(type);
    jni.typeToArrow(typeJson, schema.memoryAddress());
  }

  // BaseVector / RowVector / SelectivityVector.
  public static void baseVectorToArrow(BaseVector vector, ArrowSchema schema, ArrowArray array) {
    JniWrapper.baseVectorToArrow(vector.id(), schema.memoryAddress(), array.memoryAddress());
  }

  public static String baseVectorSerialize(List<? extends BaseVector> vector) {
    return JniWrapper.baseVectorSerialize(vector.stream().mapToLong(BaseVector::id).toArray());
  }

  /** Serialize vectors to raw binary bytes (no Base64 encoding). */
  public static byte[] baseVectorSerializeToBuf(List<? extends BaseVector> vector) {
    return JniWrapper.baseVectorSerializeToBuf(vector.stream().mapToLong(BaseVector::id).toArray());
  }

  public static Type baseVectorGetType(BaseVector vector) {
    final String typeJson = JniWrapper.baseVectorGetType(vector.id());
    return Serde.fromJson(typeJson, Type.class);
  }

  public static int baseVectorGetSize(BaseVector vector) {
    return JniWrapper.baseVectorGetSize(vector.id());
  }

  public static VectorEncoding baseVectorGetEncoding(BaseVector vector) {
    return VectorEncoding.valueOf(JniWrapper.baseVectorGetEncoding(vector.id()));
  }

  public static void baseVectorAppend(BaseVector vector, BaseVector toAppend) {
    JniWrapper.baseVectorAppend(vector.id(), toAppend.id());
  }

  public static boolean selectivityVectorIsValid(SelectivityVector vector, int idx) {
    return JniWrapper.selectivityVectorIsValid(vector.id(), idx);
  }

  public BaseVector createEmptyBaseVector(Type type) {
    final String typeJson = Serde.toJson(type);
    return baseVectorCreateById(jni.createEmptyBaseVector(typeJson));
  }

  public BaseVector arrowToBaseVector(ArrowSchema schema, ArrowArray array) {
    try {
      return baseVectorCreateById(jni.arrowToBaseVector(schema.memoryAddress(), array.memoryAddress()));
    } finally {
      schema.close();
      array.close();
    }
  }

  public List<BaseVector> baseVectorDeserialize(String serialized) {
    return Arrays.stream(jni.baseVectorDeserialize(serialized))
        .mapToObj(this::baseVectorCreateById)
        .collect(Collectors.toList());
  }

  public List<BaseVector> baseVectorDeserializeFromBuf(byte[] buf) {
    return Arrays.stream(jni.baseVectorDeserializeFromBuf(buf))
        .mapToObj(this::baseVectorCreateById)
        .collect(Collectors.toList());
  }

  public BaseVector baseVectorWrapInConstant(BaseVector vector, int length, int index) {
    return baseVectorCreateById(jni.baseVectorWrapInConstant(vector.id(), length, index));
  }

  public BaseVector baseVectorSlice(BaseVector vector, int offset, int length) {
    return baseVectorCreateById(jni.baseVectorSlice(vector.id(), offset, length));
  }

  public List<RowVector> rowVectorPartitionByKeys(
      RowVector vector, List<Integer> keyChannels, int maxPartitions) {
    final int[] keyChannelArray = keyChannels.stream().mapToInt(i -> i).toArray();
    final long[] vids = jni.rowVectorPartitionByKeys(vector.id(), keyChannelArray, maxPartitions);
    return Arrays.stream(vids)
        .mapToObj(this::baseVectorCreateById)
        .map(BaseVector::asRowVector)
        .collect(Collectors.toList());
  }

  public PartitionFunction createPartitionFunction(
      PartitionFunctionSpec spec, int numPartitions, boolean localExchange) {
    final String specJson = Serde.toPrettyJson(spec);
    return new PartitionFunction(
        this, jni.createPartitionFunction(specJson, numPartitions, localExchange));
  }

  public int[] partitionFunctionPartition(
      PartitionFunction partitionFunction, RowVector rowVector) {
    return jni.partitionFunctionPartition(partitionFunction.id(), rowVector.id());
  }

  public List<BaseVector> baseVectorWrapPartitions(
      BaseVector vector, int[] partitions, int numPartitions) {
    final long[] vids = jni.baseVectorWrapPartitions(vector.id(), partitions, numPartitions);
    return Arrays.stream(vids)
        .mapToObj(
            vid -> {
              if (vid == INVALID_OBJECT_HANDLE) {
                return null;
              }
              return baseVectorCreateById(vid);
            })
        .collect(Collectors.toList());
  }

  public BaseVector flattenVector(BaseVector vector) {
    return baseVectorCreateById(jni.baseVectorFlatten(vector.id()));
  }

  public SelectivityVector createSelectivityVector(int length) {
    return new SelectivityVector(jni.createSelectivityVector(length));
  }

  // TableWrite.
  public static RowType tableWriteTraitsOutputType() {
    final String typeJson = JniWrapper.tableWriteTraitsOutputType();
    final RowType type = Serde.fromJson(typeJson, RowType.class);
    return type;
  }

  public RowType tableWriteTraitsOutputTypeFromColumnStatsSpec(ColumnStatsSpec columnStatsSpec) {
    final String columnStatsSpecJson = Serde.toJson(columnStatsSpec);
    final String typeJson = jni.tableWriteTraitsOutputTypeFromColumnStatsSpec(columnStatsSpecJson);
    final RowType type = Serde.fromJson(typeJson, RowType.class);
    return type;
  }

  // PlanNode.
  public static String planNodeToString(PlanNode planNode, boolean detailed, boolean recursive) {
    return JniWrapper.planNodeToString(Serde.toPrettyJson(planNode), detailed, recursive);
  }

  // Serde.
  public static ISerializable iSerializableAsJava(ISerializableCo co) {
    final String json = JniWrapper.iSerializableAsJava(co.id());
    return Serde.fromJson(json, ISerializable.class);
  }

  public ISerializableCo iSerializableAsCpp(ISerializable iSerializable) {
    final String json = Serde.toPrettyJson(iSerializable);
    return new ISerializableCo(jni.iSerializableAsCpp(json));
  }

  @VisibleForTesting
  public UpIterator createUpIteratorWithExternalStream(ExternalStream es) {
    return new GenericUpIterator(this, jni.createUpIteratorWithExternalStream(es.id()));
  }

  private BaseVector baseVectorCreateById(long id) {
    final VectorEncoding encoding = VectorEncoding.valueOf(JniWrapper.baseVectorGetEncoding(id));
    return BaseVector.createById(this, id, encoding);
  }
}
