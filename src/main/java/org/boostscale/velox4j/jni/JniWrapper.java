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

import com.google.common.annotations.VisibleForTesting;

import org.boostscale.velox4j.iterator.DownIterator;
import org.boostscale.velox4j.memory.AllocationListener;

/** JNI native method declarations for both global and session-aware APIs. */
final class JniWrapper {
  private final long sessionId;

  JniWrapper(long sessionId) {
    this.sessionId = sessionId;
  }

  @CalledFromNative
  public long sessionId() {
    return sessionId;
  }

  // Global initialization.
  static native void initialize(String globalConfJson);

  // For Memory.
  static native long createMemoryManager(AllocationListener listener);

  // For Lifecycle.
  static native long createSession(long memoryManagerId);

  static native void releaseCppObject(long objectId);

  // Expression evaluation.
  native long createEvaluator(String evalJson);

  native long evaluatorEval(long evaluatorId, long selectivityVectorId, long rvId);

  // Plan execution.
  native long createQueryExecutor(String queryJson);

  native long queryExecutorExecute(long id);

  // For UpIterator.
  static native int upIteratorAdvance(long id);

  static native void upIteratorWait(long id);

  native long upIteratorGet(long id);

  // For DownIterator.
  static native void blockingQueuePut(long id, long rvId);

  static native void blockingQueueNoMoreInput(long id);

  native long createExternalStreamFromDownIterator(DownIterator itr);

  native long createBlockingQueue();

  // For SerialTask.
  static native void serialTaskAddSplit(
      long id, String planNodeId, int groupId, String connectorSplitJson);

  static native void serialTaskNoMoreSplits(long id, String planNodeId);

  static native String serialTaskCollectStats(long id);

  // For Variant.
  static native String variantInferType(String json);

  // For Type.
  static native String arrowToType(long cSchema);

  native void typeToArrow(String typeJson, long cSchema);

  // For BaseVector / RowVector / SelectivityVector.
  static native void baseVectorToArrow(long rvid, long cSchema, long cArray);

  static native String baseVectorSerialize(long[] id);

  // Serialize vectors to raw binary (no Base64 encoding). More efficient for network transport.
  static native byte[] baseVectorSerializeToBuf(long[] id);

  static native String baseVectorGetType(long id);

  static native int baseVectorGetSize(long id);

  static native String baseVectorGetEncoding(long id);

  static native void baseVectorAppend(long id, long toAppendId);

  static native boolean selectivityVectorIsValid(long id, int idx);

  native long createEmptyBaseVector(String typeJson);

  native long arrowToBaseVector(long cSchema, long cArray);

  native long[] baseVectorDeserialize(String serialized);

  // Deserialize vectors from raw binary (no Base64 decoding). More efficient for network transport.
  native long[] baseVectorDeserializeFromBuf(byte[] buf);

  native long baseVectorWrapInConstant(long id, int length, int index);

  native long baseVectorSlice(long id, int offset, int length);

  native long baseVectorFlatten(long id);

  native long[] rowVectorPartitionByKeys(long id, int[] keyChannels, int maxPartitions);

  native long createPartitionFunction(String specJson, int numPartitions, boolean localExchange);

  native int[] partitionFunctionPartition(long partitionFunctionId, long rowVectorId);

  native long[] baseVectorWrapPartitions(long vectorId, int[] partitions, int numPartitions);

  native long createSelectivityVector(int length);

  // For TableWrite.
  static native String tableWriteTraitsOutputType();

  native String tableWriteTraitsOutputTypeFromColumnStatsSpec(String columnStatsSpecJson);

  // For PlanNode.
  static native String planNodeToString(String planNodeJson, boolean detailed, boolean recursive);

  // For serde.
  static native String iSerializableAsJava(long id);

  static native String variantAsJava(long id);

  native long iSerializableAsCpp(String json);

  native long variantAsCpp(String json);

  native long variantToVector(String typeJson, String variantJson);

  @VisibleForTesting
  native long createUpIteratorWithExternalStream(long id);
}
