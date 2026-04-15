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

import java.util.List;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.connector.ConnectorSplit;
import org.boostscale.velox4j.connector.ExternalStreams;
import org.boostscale.velox4j.data.BaseVector;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.data.SelectivityVector;
import org.boostscale.velox4j.data.VectorEncoding;
import org.boostscale.velox4j.iterator.UpIterator;
import org.boostscale.velox4j.memory.AllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.plan.PlanNode;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.query.SerialTaskStats;
import org.boostscale.velox4j.serde.Serde;
import org.boostscale.velox4j.serializable.ISerializable;
import org.boostscale.velox4j.serializable.ISerializableCo;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.variant.Variant;
import org.boostscale.velox4j.variant.VariantCo;

/** The higher-level API over static native methods on {@link JniWrapper}. */
public class StaticJniApi {
  private static final StaticJniApi INSTANCE = new StaticJniApi();

  public static StaticJniApi get() {
    return INSTANCE;
  }

  private StaticJniApi() {}

  public void initialize(Config globalConf) {
    JniWrapper.initialize(Serde.toPrettyJson(globalConf));
  }

  public MemoryManager createMemoryManager(AllocationListener listener) {
    return new MemoryManager(JniWrapper.createMemoryManager(listener));
  }

  public LocalSession createSession(MemoryManager memoryManager) {
    return new LocalSession(JniWrapper.createSession(memoryManager.id()));
  }

  public void releaseCppObject(CppObject obj) {
    JniWrapper.releaseCppObject(obj.id());
  }

  public UpIterator.State upIteratorAdvance(UpIterator itr) {
    return UpIterator.State.get(JniWrapper.upIteratorAdvance(itr.id()));
  }

  public void upIteratorWait(UpIterator itr) {
    JniWrapper.upIteratorWait(itr.id());
  }

  public void blockingQueuePut(ExternalStreams.BlockingQueue queue, RowVector rowVector) {
    JniWrapper.blockingQueuePut(queue.id(), rowVector.id());
  }

  public void blockingQueueNoMoreInput(ExternalStreams.BlockingQueue queue) {
    JniWrapper.blockingQueueNoMoreInput(queue.id());
  }

  public void serialTaskAddSplit(
      SerialTask serialTask, String planNodeId, int groupId, ConnectorSplit split) {
    final String splitJson = Serde.toJson(split);
    JniWrapper.serialTaskAddSplit(serialTask.id(), planNodeId, groupId, splitJson);
  }

  public void serialTaskNoMoreSplits(SerialTask serialTask, String planNodeId) {
    JniWrapper.serialTaskNoMoreSplits(serialTask.id(), planNodeId);
  }

  public SerialTaskStats serialTaskCollectStats(SerialTask serialTask) {
    final String statsJson = JniWrapper.serialTaskCollectStats(serialTask.id());
    return SerialTaskStats.fromJson(statsJson);
  }

  public Type variantInferType(Variant variant) {
    final String variantJson = Serde.toJson(variant);
    final String typeJson = JniWrapper.variantInferType(variantJson);
    return Serde.fromJson(typeJson, Type.class);
  }

  public Type arrowToRowType(ArrowSchema schema) {
    try {
      final String typeJson = JniWrapper.arrowToType(schema.memoryAddress());
      return Serde.fromJson(typeJson, Type.class);
    } finally {
      schema.close();
    }
  }

  public void baseVectorToArrow(BaseVector vector, ArrowSchema schema, ArrowArray array) {
    JniWrapper.baseVectorToArrow(vector.id(), schema.memoryAddress(), array.memoryAddress());
  }

  public String baseVectorSerialize(List<? extends BaseVector> vector) {
    return JniWrapper.baseVectorSerialize(vector.stream().mapToLong(BaseVector::id).toArray());
  }

  /** Serialize vectors to raw binary bytes (no Base64 encoding). */
  public byte[] baseVectorSerializeToBuf(List<? extends BaseVector> vector) {
    return JniWrapper.baseVectorSerializeToBuf(vector.stream().mapToLong(BaseVector::id).toArray());
  }

  public Type baseVectorGetType(BaseVector vector) {
    final String typeJson = JniWrapper.baseVectorGetType(vector.id());
    return Serde.fromJson(typeJson, Type.class);
  }

  public int baseVectorGetSize(BaseVector vector) {
    return JniWrapper.baseVectorGetSize(vector.id());
  }

  public VectorEncoding baseVectorGetEncoding(BaseVector vector) {
    return VectorEncoding.valueOf(JniWrapper.baseVectorGetEncoding(vector.id()));
  }

  public void baseVectorAppend(BaseVector vector, BaseVector toAppend) {
    JniWrapper.baseVectorAppend(vector.id(), toAppend.id());
  }

  public boolean selectivityVectorIsValid(SelectivityVector vector, int idx) {
    return JniWrapper.selectivityVectorIsValid(vector.id(), idx);
  }

  public RowType tableWriteTraitsOutputType() {
    final String typeJson = JniWrapper.tableWriteTraitsOutputType();
    final RowType type = Serde.fromJson(typeJson, RowType.class);
    return type;
  }

  public String planNodeToString(PlanNode planNode, boolean detailed, boolean recursive) {
    return JniWrapper.planNodeToString(Serde.toPrettyJson(planNode), detailed, recursive);
  }

  public ISerializable iSerializableAsJava(ISerializableCo co) {
    final String json = JniWrapper.iSerializableAsJava(co.id());
    return Serde.fromJson(json, ISerializable.class);
  }

  public Variant variantAsJava(VariantCo co) {
    final String json = JniWrapper.variantAsJava(co.id());
    return Serde.fromJson(json, Variant.class);
  }
}
