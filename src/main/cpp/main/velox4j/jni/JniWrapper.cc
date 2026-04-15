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

#include "velox4j/jni/JniWrapper.h"
#include <velox/common/encode/Base64.h>
#include <velox/common/memory/Memory.h>
#include <velox/connectors/hive/PartitionIdGenerator.h>
#include <velox/core/PlanNode.h>
#include <velox/exec/OperatorUtils.h>
#include <velox/exec/TableWriter.h>
#include <velox/vector/VariantToVector.h>
#include <velox/vector/VectorSaver.h>

#include "velox4j/arrow/Arrow.h"
#include "velox4j/config/Config.h"
#include "velox4j/connector/ExternalStream.h"
#include "velox4j/eval/Evaluator.h"
#include "velox4j/init/Init.h"
#include "velox4j/iterator/BlockingQueue.h"
#include "velox4j/iterator/DownIterator.h"
#include "velox4j/iterator/UpIterator.h"
#include "velox4j/jni/JniCommon.h"
#include "velox4j/jni/JniError.h"
#include "velox4j/lifecycle/Session.h"
#include "velox4j/memory/JavaAllocationListener.h"
#include "velox4j/query/QueryExecutor.h"
#include "velox4j/vector/Vectors.h"

namespace velox4j {
using namespace facebook::velox;

namespace {
const char* kClassName = "org/boostscale/velox4j/jni/JniWrapper";

void initialize0(JNIEnv* env, jclass clazz, jstring globalConfJson) {
  JNI_METHOD_START
  spotify::jni::JavaString jGlobalConfJson{env, globalConfJson};
  auto dynamic = folly::parseJson(jGlobalConfJson.get());
  auto confArray = ConfigArray::create(dynamic);
  initialize(confArray);
  JNI_METHOD_END()
}

jlong createMemoryManager(JNIEnv* env, jclass clazz, jobject jListener) {
  JNI_METHOD_START
  auto listener = std::make_unique<BlockAllocationListener>(
      std::make_unique<JavaAllocationListener>(env, jListener), 8 << 10 << 10);
  auto mm = std::make_shared<MemoryManager>(std::move(listener));
  return ObjectStore::global()->save(mm);
  JNI_METHOD_END(-1L)
}

jlong createSession(JNIEnv* env, jclass clazz, long memoryManagerId) {
  JNI_METHOD_START
  auto mm = ObjectStore::retrieve<MemoryManager>(memoryManagerId);
  return ObjectStore::global()->save(std::make_shared<Session>(mm.get()));
  JNI_METHOD_END(-1L)
}

void releaseCppObject(JNIEnv* env, jclass clazz, jlong objId) {
  JNI_METHOD_START
  ObjectStore::release(objId);
  JNI_METHOD_END()
}

/// Get the Velox4J session object that is associated with the current
/// JniWrapper.
Session* sessionOf(JNIEnv* env, jobject javaThis) {
  static const auto* clazz = jniClassRegistry()->get(kClassName);
  static jmethodID methodId = clazz->getMethod("sessionId");
  const jlong sessionId = env->CallLongMethod(javaThis, methodId);
  checkException(env);
  return ObjectStore::retrieve<Session>(sessionId).get();
}

jlong createEvaluator(JNIEnv* env, jobject javaThis, jstring evalJson) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  spotify::jni::JavaString jExprJson{env, evalJson};
  auto evaluationSerdePool = session->memoryManager()->getVeloxPool(
      "Evaluation Serde Memory Pool", memory::MemoryPool::Kind::kLeaf);
  auto exprDynamic = folly::parseJson(jExprJson.get());
  auto evaluation =
      ISerializable::deserialize<Evaluation>(exprDynamic, evaluationSerdePool);
  auto evaluator =
      std::make_shared<Evaluator>(session->memoryManager(), evaluation);
  return sessionOf(env, javaThis)->objectStore()->save(evaluator);
  JNI_METHOD_END(-1L)
}

jlong evaluatorEval(
    JNIEnv* env,
    jobject javaThis,
    jlong evaluatorId,
    jlong selectivityVectorId,
    jlong rvId) {
  JNI_METHOD_START
  auto evaluator = ObjectStore::retrieve<Evaluator>(evaluatorId);
  auto selectivityVector =
      ObjectStore::retrieve<SelectivityVector>(selectivityVectorId);
  auto input = ObjectStore::retrieve<RowVector>(rvId);
  return sessionOf(env, javaThis)
      ->objectStore()
      ->save(evaluator->eval(*selectivityVector, *input));
  JNI_METHOD_END(-1L)
}

jlong createQueryExecutor(JNIEnv* env, jobject javaThis, jstring queryJson) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  spotify::jni::JavaString jQueryJson{env, queryJson};
  auto querySerdePool = session->memoryManager()->getVeloxPool(
      fmt::format("Query Serde Memory Pool"), memory::MemoryPool::Kind::kLeaf);
  // Keep the pool alive until the task is finished.
  auto queryDynamic = folly::parseJson(jQueryJson.get());
  auto query = ISerializable::deserialize<Query>(queryDynamic, querySerdePool);
  auto exec = std::make_shared<QueryExecutor>(session->memoryManager(), query);
  return sessionOf(env, javaThis)->objectStore()->save(exec);
  JNI_METHOD_END(-1L)
}

jlong queryExecutorExecute(
    JNIEnv* env,
    jobject javaThis,
    jlong queryExecutorId) {
  JNI_METHOD_START
  auto exec = ObjectStore::retrieve<QueryExecutor>(queryExecutorId);
  return sessionOf(env, javaThis)
      ->objectStore()
      ->save<SerialTask>(exec->execute());
  JNI_METHOD_END(-1L)
}

jint upIteratorAdvance(JNIEnv* env, jclass clazz, jlong itrId) {
  JNI_METHOD_START
  auto itr = ObjectStore::retrieve<UpIterator>(itrId);
  return static_cast<jint>(itr->advance());
  JNI_METHOD_END(-1)
}

void upIteratorWait(JNIEnv* env, jclass clazz, jlong itrId) {
  JNI_METHOD_START
  auto itr = ObjectStore::retrieve<UpIterator>(itrId);
  itr->wait();
  JNI_METHOD_END()
}

jlong upIteratorGet(JNIEnv* env, jobject javaThis, jlong itrId) {
  JNI_METHOD_START
  auto itr = ObjectStore::retrieve<UpIterator>(itrId);
  return sessionOf(env, javaThis)->objectStore()->save(itr->get());
  JNI_METHOD_END(-1L)
}

void blockingQueuePut(JNIEnv* env, jclass clazz, jlong queueId, jlong rvId) {
  JNI_METHOD_START
  auto queue = ObjectStore::retrieve<BlockingQueue>(queueId);
  auto rv = ObjectStore::retrieve<RowVector>(rvId);
  queue->put(rv);
  JNI_METHOD_END()
}

void blockingQueueNoMoreInput(JNIEnv* env, jclass clazz, jlong queueId) {
  JNI_METHOD_START
  auto queue = ObjectStore::retrieve<BlockingQueue>(queueId);
  queue->noMoreInput();
  JNI_METHOD_END()
}

jlong createExternalStreamFromDownIterator(
    JNIEnv* env,
    jobject javaThis,
    jobject itrRef) {
  JNI_METHOD_START
  auto es = std::make_shared<DownIterator>(env, itrRef);
  return sessionOf(env, javaThis)->objectStore()->save(es);
  JNI_METHOD_END(-1L)
}

jlong createBlockingQueue(JNIEnv* env, jobject javaThis) {
  JNI_METHOD_START
  auto queue = std::make_shared<BlockingQueue>();
  return sessionOf(env, javaThis)->objectStore()->save(queue);
  JNI_METHOD_END(-1L)
}

void serialTaskAddSplit(
    JNIEnv* env,
    jclass clazz,
    jlong stId,
    jstring planNodeId,
    jint groupId,
    jstring connectorSplitJson) {
  JNI_METHOD_START
  auto serialTask = ObjectStore::retrieve<SerialTask>(stId);
  spotify::jni::JavaString jPlanNodeId{env, planNodeId};
  spotify::jni::JavaString jConnectorSplitJson{env, connectorSplitJson};
  auto jConnectorSplitDynamic = folly::parseJson(jConnectorSplitJson.get());
  auto connectorSplit = std::const_pointer_cast<connector::ConnectorSplit>(
      ISerializable::deserialize<connector::ConnectorSplit>(
          jConnectorSplitDynamic));
  serialTask->addSplit(jPlanNodeId.get(), groupId, connectorSplit);
  JNI_METHOD_END()
}

void serialTaskNoMoreSplits(
    JNIEnv* env,
    jclass clazz,
    jlong stId,
    jstring planNodeId) {
  JNI_METHOD_START
  auto serialTask = ObjectStore::retrieve<SerialTask>(stId);
  spotify::jni::JavaString jPlanNodeId{env, planNodeId};
  serialTask->noMoreSplits(jPlanNodeId.get());
  JNI_METHOD_END()
}

jstring serialTaskCollectStats(JNIEnv* env, jclass clazz, jlong stId) {
  JNI_METHOD_START
  auto serialTask = ObjectStore::retrieve<SerialTask>(stId);
  const auto stats = serialTask->collectStats();
  const auto statsDynamic = stats->toJson();
  const auto statsJson = folly::toPrettyJson(statsDynamic);
  return env->NewStringUTF(statsJson.data());
  JNI_METHOD_END(nullptr)
}

jstring variantInferType(JNIEnv* env, jclass clazz, jstring json) {
  JNI_METHOD_START
  spotify::jni::JavaString jJson{env, json};
  auto dynamic = folly::parseJson(jJson.get());
  auto deserialized = variant::create(dynamic);
  auto type = deserialized.inferType();
  auto serializedDynamic = type->serialize();
  auto typeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(typeJson.data());
  JNI_METHOD_END(nullptr);
}

jstring variantAsJava(JNIEnv* env, jclass clazz, jlong id) {
  JNI_METHOD_START
  auto v = ObjectStore::retrieve<variant>(id);
  auto serializedDynamic = v->serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
  JNI_METHOD_END(nullptr)
}

jlong variantAsCpp(JNIEnv* env, jobject javaThis, jstring json) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  spotify::jni::JavaString jJson{env, json};
  auto dynamic = folly::parseJson(jJson.get());
  auto deserialized = variant::create(dynamic);
  return session->objectStore()->save(std::make_shared<variant>(deserialized));
  JNI_METHOD_END(-1)
}

jlong variantToVector(
    JNIEnv* env,
    jobject javaThis,
    jstring typeJson,
    jstring variantJson) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  auto vectorPool = session->memoryManager()->getVeloxPool(
      "BaseVector Memory Pool", memory::MemoryPool::Kind::kLeaf);
  spotify::jni::JavaString jTypeJson{env, typeJson};
  spotify::jni::JavaString jVariantJson{env, variantJson};
  auto type = Type::create(folly::parseJson(jTypeJson.get()));
  auto variant = variant::create(folly::parseJson(jVariantJson.get()));
  auto variantVector =
      facebook::velox::variantToVector(type, variant, vectorPool);
  return session->objectStore()->save(variantVector);
  JNI_METHOD_END(-1)
}

jstring arrowToType(JNIEnv* env, jclass clazz, jlong cSchema) {
  JNI_METHOD_START
  auto type = fromArrowToType(reinterpret_cast<struct ArrowSchema*>(cSchema));
  auto serializedDynamic = type->serialize();
  auto typeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(typeJson.data());
  JNI_METHOD_END(nullptr)
}

void typeToArrow(
    JNIEnv* env,
    jobject javaThis,
    jstring typeJson,
    jlong cSchema) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  spotify::jni::JavaString jTypeJson{env, typeJson};
  auto dynamic = folly::parseJson(jTypeJson.get());
  auto type = Type::create(dynamic);
  auto typePool = session->memoryManager()->getVeloxPool(
      "Type Memory Pool", memory::MemoryPool::Kind::kLeaf);
  fromTypeToArrow(
      typePool, type, reinterpret_cast<struct ArrowSchema*>(cSchema));
  JNI_METHOD_END()
}

void baseVectorToArrow(
    JNIEnv* env,
    jclass clazz,
    jlong vid,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  fromBaseVectorToArrow(
      vector,
      reinterpret_cast<struct ArrowSchema*>(cSchema),
      reinterpret_cast<struct ArrowArray*>(cArray));
  JNI_METHOD_END()
}

jstring baseVectorSerialize(JNIEnv* env, jclass clazz, jlongArray vids) {
  JNI_METHOD_START
  std::ostringstream out;
  auto safeArray = getLongArrayElementsSafe(env, vids);
  for (int i = 0; i < safeArray.length(); ++i) {
    const jlong& vid = safeArray.elems()[i];
    auto vector = ObjectStore::retrieve<BaseVector>(vid);
    saveVector(*vector, out);
  }
  auto serializedData = out.str();
  auto encoded =
      encoding::Base64::encode(serializedData.data(), serializedData.size());
  return env->NewStringUTF(encoded.data());
  JNI_METHOD_END(nullptr)
}

jbyteArray
baseVectorSerializeToBuf(JNIEnv* env, jclass clazz, jlongArray vids) {
  JNI_METHOD_START
  std::ostringstream out;
  auto safeArray = getLongArrayElementsSafe(env, vids);
  for (int i = 0; i < safeArray.length(); ++i) {
    const jlong& vid = safeArray.elems()[i];
    auto vector = ObjectStore::retrieve<BaseVector>(vid);
    saveVector(*vector, out);
  }
  auto serializedData = out.str();
  jbyteArray byteArray = env->NewByteArray(serializedData.size());
  env->SetByteArrayRegion(
      byteArray,
      0,
      serializedData.size(),
      reinterpret_cast<const jbyte*>(serializedData.data()));
  return byteArray;
  JNI_METHOD_END(nullptr)
}

jstring baseVectorGetType(JNIEnv* env, jclass clazz, jlong vid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  auto serializedDynamic = vector->type()->serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
  JNI_METHOD_END(nullptr)
}

jint baseVectorGetSize(JNIEnv* env, jclass clazz, jlong vid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  return static_cast<jint>(vector->size());
  JNI_METHOD_END(-1)
}

jstring baseVectorGetEncoding(JNIEnv* env, jclass clazz, jlong vid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  auto name = VectorEncoding::mapSimpleToName(vector->encoding());
  return env->NewStringUTF(name.data());
  JNI_METHOD_END(nullptr)
}

void baseVectorAppend(JNIEnv* env, jclass clazz, jlong vid, jlong toAppendVid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  auto toAppend = ObjectStore::retrieve<BaseVector>(toAppendVid);
  vector->append(toAppend.get());
  JNI_METHOD_END()
}

jboolean
selectivityVectorIsValid(JNIEnv* env, jclass clazz, jlong svId, jint idx) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<SelectivityVector>(svId);
  auto valid = vector->isValid(static_cast<vector_size_t>(idx));
  return static_cast<jboolean>(valid);
  JNI_METHOD_END(false)
}

jlong createEmptyBaseVector(JNIEnv* env, jobject javaThis, jstring typeJson) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  spotify::jni::JavaString jTypeJson{env, typeJson};
  auto dynamic = folly::parseJson(jTypeJson.get());
  auto type = Type::create(dynamic);
  auto vectorPool = session->memoryManager()->getVeloxPool(
      "BaseVector Memory Pool", memory::MemoryPool::Kind::kLeaf);
  auto vector = BaseVector::create(type, 0, vectorPool);
  return session->objectStore()->save(vector);
  JNI_METHOD_END(-1L)
}

jlong arrowToBaseVector(
    JNIEnv* env,
    jobject javaThis,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  // TODO Session memory pool.
  auto session = sessionOf(env, javaThis);
  auto pool = session->memoryManager()->getVeloxPool(
      "Arrow Import Memory Pool", memory::MemoryPool::Kind::kLeaf);
  auto vector = fromArrowToBaseVector(
      pool,
      reinterpret_cast<struct ArrowSchema*>(cSchema),
      reinterpret_cast<struct ArrowArray*>(cArray));
  return session->objectStore()->save(vector);
  JNI_METHOD_END(-1L)
}

jlongArray
baseVectorDeserialize(JNIEnv* env, jobject javaThis, jstring serialized) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  spotify::jni::JavaString jSerialized{env, serialized};
  auto decoded = encoding::Base64::decode(jSerialized.get());
  std::istringstream dataStream(decoded);
  auto pool = session->memoryManager()->getVeloxPool(
      "Decoding Memory Pool", memory::MemoryPool::Kind::kLeaf);
  std::vector<ObjectHandle> vids{};
  while (dataStream.tellg() < decoded.size()) {
    const VectorPtr& vector = restoreVector(dataStream, pool);
    const ObjectHandle vid = session->objectStore()->save(vector);
    vids.push_back(vid);
  }
  const jsize& len = static_cast<jsize>(vids.size());
  const jlongArray& out = env->NewLongArray(len);
  env->SetLongArrayRegion(out, 0, len, vids.data());
  return out;
  JNI_METHOD_END(nullptr)
}

// Deserialize vectors from a raw byte array (no Base64 decoding).
jlongArray
baseVectorDeserializeFromBuf(JNIEnv* env, jobject javaThis, jbyteArray buf) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  auto safeArray = getByteArrayElementsSafe(env, buf);
  std::string data(
      reinterpret_cast<const char*>(safeArray.elems()), safeArray.length());
  std::istringstream dataStream(data);
  auto pool = session->memoryManager()->getVeloxPool(
      "Decoding Memory Pool", memory::MemoryPool::Kind::kLeaf);
  std::vector<ObjectHandle> vids{};
  while (dataStream.tellg() < data.size()) {
    const VectorPtr& vector = restoreVector(dataStream, pool);
    const ObjectHandle vid = session->objectStore()->save(vector);
    vids.push_back(vid);
  }
  const jsize& outLen = static_cast<jsize>(vids.size());
  const jlongArray& out = env->NewLongArray(outLen);
  env->SetLongArrayRegion(out, 0, outLen, vids.data());
  return out;
  JNI_METHOD_END(nullptr)
}

jlong baseVectorWrapInConstant(
    JNIEnv* env,
    jobject javaThis,
    jlong vid,
    jint length,
    jint index) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  auto constVector = BaseVector::wrapInConstant(length, index, vector);
  return sessionOf(env, javaThis)->objectStore()->save(constVector);
  JNI_METHOD_END(-1)
}

jlong baseVectorSlice(
    JNIEnv* env,
    jobject javaThis,
    jlong vid,
    jint offset,
    jint length) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  auto slicedVector = vector->slice(offset, length);
  return sessionOf(env, javaThis)->objectStore()->save(slicedVector);
  JNI_METHOD_END(-1)
}

jlong baseVectorFlatten(JNIEnv* env, jobject javaThis, jlong vid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  flattenVector(vector, vector->size());
  return sessionOf(env, javaThis)->objectStore()->save(vector);
  JNI_METHOD_END(-1)
}

jlongArray rowVectorPartitionByKeys(
    JNIEnv* env,
    jobject javaThis,
    jlong vid,
    jintArray jKeyChannels,
    jint maxPartitions) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  auto pool = session->memoryManager()->getVeloxPool(
      "Partition By Keys Memory Pool", memory::MemoryPool::Kind::kLeaf);
  const auto inputRowVector = ObjectStore::retrieve<RowVector>(vid);
  const auto inputNumRows = inputRowVector->size();

  auto safeArray = getIntArrayElementsSafe(env, jKeyChannels);
  std::vector<column_index_t> keyChannels(safeArray.length());
  for (jsize i = 0; i < safeArray.length(); ++i) {
    keyChannels[i] = safeArray.elems()[i];
  }

  VELOX_USER_CHECK_GT(maxPartitions, 0, "maxPartitions must be positive");
  connector::hive::PartitionIdGenerator idGen{
      asRowType(inputRowVector->type()),
      keyChannels,
      static_cast<uint32_t>(maxPartitions),
      pool};

  raw_vector<uint64_t> partitionIds{};
  idGen.run(inputRowVector, partitionIds);
  const auto numPartitions = idGen.numPartitions();
  VELOX_CHECK_EQ(
      partitionIds.size(),
      inputRowVector->size(),
      "Mismatched number of partition ids");

  std::vector<vector_size_t> partitionSizes(numPartitions);
  std::vector<BufferPtr> partitionRows(numPartitions);
  std::vector<vector_size_t*> rawPartitionRows(numPartitions);
  std::fill(partitionSizes.begin(), partitionSizes.end(), 0);

  for (auto row = 0; row < inputNumRows; ++row) {
    const auto partitionId = partitionIds[row];
    ++partitionSizes[partitionId];
  }

  for (int partitionId = 0; partitionId < numPartitions; ++partitionId) {
    partitionRows[partitionId] =
        allocateIndices(partitionSizes[partitionId], pool);
    rawPartitionRows[partitionId] =
        partitionRows[partitionId]->asMutable<vector_size_t>();
  }

  std::vector<vector_size_t> partitionNextRowOffset(numPartitions);
  std::fill(partitionNextRowOffset.begin(), partitionNextRowOffset.end(), 0);
  for (auto row = 0; row < inputNumRows; ++row) {
    const auto partitionId = partitionIds[row];
    rawPartitionRows[partitionId][partitionNextRowOffset[partitionId]] = row;
    ++partitionNextRowOffset[partitionId];
  }

  std::vector<jlong> outVector(numPartitions);

  for (int partitionId = 0; partitionId < numPartitions; ++partitionId) {
    const vector_size_t partitionSize = partitionSizes[partitionId];
    if (partitionSize == 0) {
      continue;
    }

    const RowVectorPtr rowVector = partitionSize == inputNumRows
        ? inputRowVector
        : exec::wrap(partitionSize, partitionRows[partitionId], inputRowVector);
    outVector[partitionId] = session->objectStore()->save(rowVector);
  }

  const jlongArray out = env->NewLongArray(outVector.size());
  env->SetLongArrayRegion(out, 0, outVector.size(), outVector.data());
  return out;

  JNI_METHOD_END(nullptr)
}

jlongArray baseVectorWrapPartitions(
    JNIEnv* env,
    jobject javaThis,
    jlong vectorId,
    jintArray jPartitions,
    jint numPartitions) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  auto pool = session->memoryManager()->getVeloxPool(
      "Wrap Partitions Memory Pool", memory::MemoryPool::Kind::kLeaf);
  VectorPtr vector = ObjectStore::retrieve<BaseVector>(vectorId);
  flattenVector(vector, vector->size());
  const auto inputNumRows = vector->size();
  auto safeArray = getIntArrayElementsSafe(env, jPartitions);

  std::vector<jlong> outVector(numPartitions, 0);
  VELOX_USER_CHECK_EQ(
      safeArray.length(),
      inputNumRows,
      "Expected one partition id per input row");

  std::vector<vector_size_t> partitionSizes(numPartitions);
  std::vector<BufferPtr> partitionRows(numPartitions);
  std::vector<vector_size_t*> rawPartitionRows(numPartitions);
  std::fill(partitionSizes.begin(), partitionSizes.end(), 0);

  for (int row = 0; row < inputNumRows; ++row) {
    const auto partitionId = static_cast<int>(safeArray.elems()[row]);
    VELOX_USER_CHECK_GE(partitionId, 0, "partition id must be non-negative");
    VELOX_USER_CHECK_LT(
        partitionId,
        numPartitions,
        "partition id {} is out of range for {} partitions",
        partitionId,
        numPartitions);
    ++partitionSizes[partitionId];
  }

  for (int partitionId = 0; partitionId < numPartitions; ++partitionId) {
    partitionRows[partitionId] =
        allocateIndices(partitionSizes[partitionId], pool);
    rawPartitionRows[partitionId] =
        partitionRows[partitionId]->asMutable<vector_size_t>();
  }

  std::vector<vector_size_t> partitionNextRowOffset(numPartitions);
  std::fill(partitionNextRowOffset.begin(), partitionNextRowOffset.end(), 0);
  for (int row = 0; row < inputNumRows; ++row) {
    const auto partitionId = static_cast<int>(safeArray.elems()[row]);
    rawPartitionRows[partitionId][partitionNextRowOffset[partitionId]] = row;
    ++partitionNextRowOffset[partitionId];
  }

  for (int partitionId = 0; partitionId < numPartitions; ++partitionId) {
    const vector_size_t partitionSize = partitionSizes[partitionId];
    if (partitionSize == 0) {
      continue;
    }
    VectorPtr partitionVector = partitionSize == inputNumRows
        ? vector
        : wrapInDictionary(partitionSize, partitionRows[partitionId], vector);
    outVector[partitionId] = session->objectStore()->save(partitionVector);
  }

  const jlongArray out = env->NewLongArray(outVector.size());
  env->SetLongArrayRegion(out, 0, outVector.size(), outVector.data());
  return out;

  JNI_METHOD_END(nullptr)
}

jlong createPartitionFunction(
    JNIEnv* env,
    jobject javaThis,
    jstring specJson,
    jint numPartitions,
    jboolean localExchange) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  auto serdePool = session->memoryManager()->getVeloxPool(
      "Partition Function Serde Memory Pool", memory::MemoryPool::Kind::kLeaf);
  spotify::jni::JavaString jSpecJson{env, specJson};
  auto dynamic = folly::parseJson(jSpecJson.get());
  auto spec = ISerializable::deserialize<core::PartitionFunctionSpec>(
      dynamic, serdePool);
  auto function = std::shared_ptr<core::PartitionFunction>(
      spec->create(numPartitions, static_cast<bool>(localExchange)).release());
  return session->objectStore()->save(function);
  JNI_METHOD_END(-1)
}

jintArray partitionFunctionPartition(
    JNIEnv* env,
    jobject javaThis,
    jlong partitionFunctionId,
    jlong rowVectorId) {
  JNI_METHOD_START
  auto function =
      ObjectStore::retrieve<core::PartitionFunction>(partitionFunctionId);
  const auto inputRowVector = ObjectStore::retrieve<RowVector>(rowVectorId);

  std::vector<uint32_t> partitions;
  auto singlePartition = function->partition(*inputRowVector, partitions);
  std::vector<jint> outVector;
  if (singlePartition.has_value()) {
    outVector.assign(
        inputRowVector->size(), static_cast<jint>(singlePartition.value()));
  } else {
    outVector.reserve(partitions.size());
    for (const auto partition : partitions) {
      outVector.push_back(static_cast<jint>(partition));
    }
  }
  const jintArray out = env->NewIntArray(outVector.size());
  env->SetIntArrayRegion(out, 0, outVector.size(), outVector.data());
  return out;
  JNI_METHOD_END(nullptr)
}

jlong createSelectivityVector(JNIEnv* env, jobject javaThis, jint length) {
  JNI_METHOD_START
  auto vector =
      std::make_shared<SelectivityVector>(static_cast<vector_size_t>(length));
  return sessionOf(env, javaThis)->objectStore()->save(vector);
  JNI_METHOD_END(-1)
}

jstring tableWriteTraitsOutputType(JNIEnv* env, jclass clazz) {
  JNI_METHOD_START
  auto type = exec::TableWriteTraits::outputType(std::nullopt);
  auto serializedDynamic = type->serialize();
  auto typeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(typeJson.data());
  JNI_METHOD_END(nullptr)
}

jstring tableWriteTraitsOutputTypeFromColumnStatsSpec(
    JNIEnv* env,
    jobject javaThis,
    jstring columnStatsSpecJson) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  spotify::jni::JavaString jJson{env, columnStatsSpecJson};
  auto dynamic = folly::parseJson(jJson.get());
  auto serdePool = session->memoryManager()->getVeloxPool(
      "Serde Memory Pool", memory::MemoryPool::Kind::kLeaf);
  auto columnStatSpec = core::ColumnStatsSpec::create(dynamic, serdePool);
  auto type = exec::TableWriteTraits::outputType(columnStatSpec);
  auto serializedDynamic = type->serialize();
  auto typeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(typeJson.data());
  JNI_METHOD_END(nullptr)
}

jstring planNodeToString(
    JNIEnv* env,
    jclass clazz,
    jstring planNodeJson,
    jboolean detailed,
    jboolean recursive) {
  JNI_METHOD_START
  spotify::jni::JavaString jJson{env, planNodeJson};
  auto dynamic = folly::parseJson(jJson.get());
  auto planNode = ISerializable::deserialize<core::PlanNode>(dynamic);
  auto str = planNode->toString(detailed, recursive);
  return env->NewStringUTF(str.data());
  JNI_METHOD_END(nullptr)
}

jstring iSerializableAsJava(JNIEnv* env, jclass clazz, jlong id) {
  JNI_METHOD_START
  auto iSerializable = ObjectStore::retrieve<ISerializable>(id);
  auto serializedDynamic = iSerializable->serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
  JNI_METHOD_END(nullptr)
}

jlong iSerializableAsCpp(JNIEnv* env, jobject javaThis, jstring json) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  auto serdePool = session->memoryManager()->getVeloxPool(
      "Serde Memory Pool", memory::MemoryPool::Kind::kLeaf);
  spotify::jni::JavaString jJson{env, json};
  auto dynamic = folly::parseJson(jJson.get());
  auto deserialized = std::const_pointer_cast<ISerializable>(
      ISerializable::deserialize<ISerializable>(dynamic, serdePool));
  return session->objectStore()->save(deserialized);
  JNI_METHOD_END(-1)
}

class ExternalStreamAsUpIterator : public UpIterator {
 public:
  explicit ExternalStreamAsUpIterator(const std::shared_ptr<ExternalStream>& es)
      : es_(es) {}

  State advance() override {
    VELOX_CHECK_NULL(pending_);
    ContinueFuture future = ContinueFuture::makeEmpty();
    auto out = es_->read(future);
    if (out == std::nullopt) {
      VELOX_CHECK(future.valid());
      // Do not wait for the future to be fulfilled, just return.
      return State::BLOCKED;
    }
    VELOX_CHECK(!future.valid());
    if (out == nullptr) {
      return State::FINISHED;
    }
    pending_ = out.value();
    return State::AVAILABLE;
  }

  void wait() override {
    VELOX_CHECK_NULL(pending_);
    VELOX_NYI("Not implemented: {}", __func__);
  }

  RowVectorPtr get() override {
    VELOX_CHECK_NOT_NULL(
        pending_,
        "ExternalStreamAsUpIterator: No pending row vector to return. Make "
        "sure the iterator is available via member function advance() first");
    auto out = pending_;
    pending_ = nullptr;
    return out;
  }

 private:
  const std::shared_ptr<ExternalStream> es_;
  RowVectorPtr pending_{nullptr};
};

jlong createUpIteratorWithExternalStream(
    JNIEnv* env,
    jobject javaThis,
    jlong id) {
  JNI_METHOD_START
  auto es = ObjectStore::retrieve<ExternalStream>(id);
  return sessionOf(env, javaThis)
      ->objectStore()
      ->save(std::make_shared<ExternalStreamAsUpIterator>(es));
  JNI_METHOD_END(-1L)
}
} // namespace

void JniWrapper::mapFields() {}

const char* JniWrapper::getCanonicalName() const {
  return kClassName;
}

void JniWrapper::initialize(JNIEnv* env) {
  JavaClass::setClass(env);

  // Caches the sessionId Java method.
  cacheMethod(env, "sessionId", kTypeLong, nullptr);

  // All native method definitions.
  addNativeMethod(
      "initialize", (void*)initialize0, kTypeVoid, kTypeString, nullptr);
  addNativeMethod(
      "createMemoryManager",
      (void*)createMemoryManager,
      kTypeLong,
      "org/boostscale/velox4j/memory/AllocationListener",
      nullptr);
  addNativeMethod(
      "createSession", (void*)createSession, kTypeLong, kTypeLong, nullptr);
  addNativeMethod(
      "releaseCppObject",
      (void*)releaseCppObject,
      kTypeVoid,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "createEvaluator",
      (void*)createEvaluator,
      kTypeLong,
      kTypeString,
      nullptr);
  addNativeMethod(
      "evaluatorEval",
      (void*)evaluatorEval,
      kTypeLong,
      kTypeLong,
      kTypeLong,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "createQueryExecutor",
      (void*)createQueryExecutor,
      kTypeLong,
      kTypeString,
      nullptr);
  addNativeMethod(
      "queryExecutorExecute",
      (void*)queryExecutorExecute,
      kTypeLong,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "upIteratorAdvance",
      (void*)upIteratorAdvance,
      kTypeInt,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "upIteratorWait", (void*)upIteratorWait, kTypeVoid, kTypeLong, nullptr);
  addNativeMethod(
      "upIteratorGet", (void*)upIteratorGet, kTypeLong, kTypeLong, nullptr);
  addNativeMethod(
      "blockingQueuePut",
      (void*)blockingQueuePut,
      kTypeVoid,
      kTypeLong,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "blockingQueueNoMoreInput",
      (void*)blockingQueueNoMoreInput,
      kTypeVoid,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "createExternalStreamFromDownIterator",
      (void*)createExternalStreamFromDownIterator,
      kTypeLong,
      "org/boostscale/velox4j/iterator/DownIterator",
      nullptr);
  addNativeMethod(
      "createBlockingQueue", (void*)createBlockingQueue, kTypeLong, nullptr);
  addNativeMethod(
      "serialTaskAddSplit",
      (void*)serialTaskAddSplit,
      kTypeVoid,
      kTypeLong,
      kTypeString,
      kTypeInt,
      kTypeString,
      nullptr);
  addNativeMethod(
      "serialTaskNoMoreSplits",
      (void*)serialTaskNoMoreSplits,
      kTypeVoid,
      kTypeLong,
      kTypeString,
      nullptr);
  addNativeMethod(
      "serialTaskCollectStats",
      (void*)serialTaskCollectStats,
      kTypeString,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "variantInferType",
      (void*)variantInferType,
      kTypeString,
      kTypeString,
      nullptr);
  addNativeMethod(
      "variantAsJava", (void*)variantAsJava, kTypeString, kTypeLong, nullptr);
  addNativeMethod(
      "variantAsCpp", (void*)variantAsCpp, kTypeLong, kTypeString, nullptr);
  addNativeMethod(
      "variantToVector",
      (void*)variantToVector,
      kTypeLong,
      kTypeString,
      kTypeString,
      nullptr);
  addNativeMethod(
      "arrowToType", (void*)arrowToType, kTypeString, kTypeLong, nullptr);
  addNativeMethod(
      "typeToArrow",
      (void*)typeToArrow,
      kTypeVoid,
      kTypeString,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "createEmptyBaseVector",
      (void*)createEmptyBaseVector,
      kTypeLong,
      kTypeString,
      nullptr);
  addNativeMethod(
      "arrowToBaseVector",
      (void*)arrowToBaseVector,
      kTypeLong,
      kTypeLong,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "baseVectorToArrow",
      (void*)baseVectorToArrow,
      kTypeVoid,
      kTypeLong,
      kTypeLong,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "baseVectorSerialize",
      (void*)baseVectorSerialize,
      kTypeString,
      kTypeArray(kTypeLong),
      nullptr);
  addNativeMethod(
      "baseVectorSerializeToBuf",
      (void*)baseVectorSerializeToBuf,
      kTypeArray(kTypeByte),
      kTypeArray(kTypeLong),
      nullptr);
  addNativeMethod(
      "baseVectorGetType",
      (void*)baseVectorGetType,
      kTypeString,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "baseVectorGetSize",
      (void*)baseVectorGetSize,
      kTypeInt,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "baseVectorGetEncoding",
      (void*)baseVectorGetEncoding,
      kTypeString,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "baseVectorAppend",
      (void*)baseVectorAppend,
      kTypeVoid,
      kTypeLong,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "selectivityVectorIsValid",
      (void*)selectivityVectorIsValid,
      kTypeBool,
      kTypeLong,
      kTypeInt,
      nullptr);
  addNativeMethod(
      "baseVectorDeserialize",
      (void*)baseVectorDeserialize,
      kTypeArray(kTypeLong),
      kTypeString,
      nullptr);
  addNativeMethod(
      "baseVectorDeserializeFromBuf",
      (void*)baseVectorDeserializeFromBuf,
      kTypeArray(kTypeLong),
      kTypeArray(kTypeByte),
      nullptr);
  addNativeMethod(
      "baseVectorWrapInConstant",
      (void*)baseVectorWrapInConstant,
      kTypeLong,
      kTypeLong,
      kTypeInt,
      kTypeInt,
      nullptr);
  addNativeMethod(
      "baseVectorSlice",
      (void*)baseVectorSlice,
      kTypeLong,
      kTypeLong,
      kTypeInt,
      kTypeInt,
      nullptr);
  addNativeMethod(
      "baseVectorFlatten",
      (void*)baseVectorFlatten,
      kTypeLong,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "rowVectorPartitionByKeys",
      (void*)rowVectorPartitionByKeys,
      kTypeArray(kTypeLong),
      kTypeLong,
      kTypeArray(kTypeInt),
      kTypeInt,
      nullptr);
  addNativeMethod(
      "baseVectorWrapPartitions",
      (void*)baseVectorWrapPartitions,
      kTypeArray(kTypeLong),
      kTypeLong,
      kTypeArray(kTypeInt),
      kTypeInt,
      nullptr);
  addNativeMethod(
      "createPartitionFunction",
      (void*)createPartitionFunction,
      kTypeLong,
      kTypeString,
      kTypeInt,
      kTypeBool,
      nullptr);
  addNativeMethod(
      "partitionFunctionPartition",
      (void*)partitionFunctionPartition,
      kTypeArray(kTypeInt),
      kTypeLong,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "createSelectivityVector",
      (void*)createSelectivityVector,
      kTypeLong,
      kTypeInt,
      nullptr);
  addNativeMethod(
      "tableWriteTraitsOutputType",
      (void*)tableWriteTraitsOutputType,
      kTypeString,
      nullptr);
  addNativeMethod(
      "tableWriteTraitsOutputTypeFromColumnStatsSpec",
      (void*)tableWriteTraitsOutputTypeFromColumnStatsSpec,
      kTypeString,
      kTypeString,
      nullptr);
  addNativeMethod(
      "planNodeToString",
      (void*)planNodeToString,
      kTypeString,
      kTypeString,
      kTypeBool,
      kTypeBool,
      nullptr);
  addNativeMethod(
      "iSerializableAsJava",
      (void*)iSerializableAsJava,
      kTypeString,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "iSerializableAsCpp",
      (void*)iSerializableAsCpp,
      kTypeLong,
      kTypeString,
      nullptr);
  addNativeMethod(
      "createUpIteratorWithExternalStream",
      (void*)createUpIteratorWithExternalStream,
      kTypeLong,
      kTypeLong,
      nullptr);

  // Registers all native methods.
  registerNativeMethods(env);
}

} // namespace velox4j
