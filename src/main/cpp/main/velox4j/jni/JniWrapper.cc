/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "JniWrapper.h"
#include <velox/common/encode/Base64.h>
#include <velox/common/memory/Memory.h>
#include <velox/vector/VectorSaver.h>
#include "JniCommon.h"
#include "JniError.h"
#include "velox4j/arrow/Arrow.h"
#include "velox4j/connector/ExternalStream.h"
#include "velox4j/exec/QueryExecutor.h"
#include "velox4j/iterator/DownIterator.h"
#include "velox4j/lifecycle/Session.h"
#include "velox4j/memory/JavaAllocationListener.h"

namespace velox4j {
using namespace facebook::velox;

namespace {
const char* kClassName = "io/github/zhztheplayer/velox4j/jni/JniWrapper";

long createMemoryManager(JNIEnv* env, jobject javaThis, jobject jListener) {
  JNI_METHOD_START
  auto listener = std::make_unique<BlockAllocationListener>(
      std::make_unique<JavaAllocationListener>(env, jListener), 8 << 10 << 10);
  auto mm = std::make_shared<MemoryManager>(std::move(listener));
  return ObjectStore::global()->save(mm);
  JNI_METHOD_END(-1L)
}

long createSession(JNIEnv* env, jobject javaThis, long memoryManagerId) {
  JNI_METHOD_START
  auto mm = ObjectStore::retrieve<MemoryManager>(memoryManagerId);
  return ObjectStore::global()->save(std::make_unique<Session>(mm.get()));
  JNI_METHOD_END(-1L)
}

Session* sessionOf(JNIEnv* env, jobject javaThis) {
  static const auto* clazz = jniClassRegistry()->get(kClassName);
  static jmethodID methodId = clazz->getMethod("sessionId");
  const jlong sessionId = env->CallLongMethod(javaThis, methodId);
  checkException(env);
  return ObjectStore::retrieve<Session>(sessionId).get();
}

void releaseCppObject(JNIEnv* env, jobject javaThis, jlong objId) {
  JNI_METHOD_START
  ObjectStore::release(objId);
  JNI_METHOD_END()
}

jlong executeQuery(JNIEnv* env, jobject javaThis, jstring queryJson) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  spotify::jni::JavaString jQueryJson{env, queryJson};
  QueryExecutor exec{session->memoryManager(), jQueryJson.get()};
  return sessionOf(env, javaThis)->objectStore()->save(exec.execute());
  JNI_METHOD_END(-1L)
}

jboolean upIteratorHasNext(JNIEnv* env, jobject javaThis, jlong itrId) {
  JNI_METHOD_START
  auto itr = ObjectStore::retrieve<UpIterator>(itrId);
  return itr->hasNext();
  JNI_METHOD_END(false)
}

jlong upIteratorNext(JNIEnv* env, jobject javaThis, jlong itrId) {
  JNI_METHOD_START
  auto itr = ObjectStore::retrieve<UpIterator>(itrId);
  return sessionOf(env, javaThis)->objectStore()->save(itr->next());
  JNI_METHOD_END(-1L)
}

jlong downIteratorAsExternalStream(
    JNIEnv* env,
    jobject javaThis,
    jobject itrRef) {
  JNI_METHOD_START
  auto es = std::make_shared<DownIterator>(env, itrRef);
  return sessionOf(env, javaThis)->objectStore()->save(es);
  JNI_METHOD_END(-1L)
}

jstring variantInferType(JNIEnv* env, jobject javaThis, jstring json) {
  JNI_METHOD_START
  spotify::jni::JavaString jJson{env, json};
  auto dynamic = folly::parseJson(jJson.get());
  auto deserialized = variant::create(dynamic);
  auto type = deserialized.inferType();
  auto serializedDynamic = type->serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
  JNI_METHOD_END(nullptr);
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

void baseVectorToArrow(
    JNIEnv* env,
    jobject javaThis,
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

jstring baseVectorSerialize(JNIEnv* env, jobject javaThis, jlongArray vids) {
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

jstring baseVectorGetType(JNIEnv* env, jobject javaThis, jlong vid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  auto serializedDynamic = vector->type()->serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
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

jlong baseVectorNewRef(JNIEnv* env, jobject javaThis, jlong vid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  return sessionOf(env, javaThis)->objectStore()->save(vector);
  JNI_METHOD_END(-1)
}

jstring baseVectorGetEncoding(JNIEnv* env, jobject javaThis, jlong vid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  auto name = VectorEncoding::mapSimpleToName(vector->encoding());
  return env->NewStringUTF(name.data());
  JNI_METHOD_END(nullptr)
}

jstring deserializeAndSerialize(JNIEnv* env, jobject javaThis, jstring json) {
  JNI_METHOD_START
  auto session = sessionOf(env, javaThis);
  auto serdePool = session->memoryManager()->getVeloxPool(
      "Serde Memory Pool", memory::MemoryPool::Kind::kLeaf);
  spotify::jni::JavaString jJson{env, json};
  auto dynamic = folly::parseJson(jJson.get());
  auto deserialized =
      ISerializable::deserialize<ISerializable>(dynamic, serdePool);
  auto serializedDynamic = deserialized->serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
  JNI_METHOD_END(nullptr)
}

jstring
deserializeAndSerializeVariant(JNIEnv* env, jobject javaThis, jstring json) {
  JNI_METHOD_START
  spotify::jni::JavaString jJson{env, json};
  auto dynamic = folly::parseJson(jJson.get());
  auto deserialized = variant::create(dynamic);
  auto serializedDynamic = deserialized.serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
  JNI_METHOD_END(nullptr)
}

class ExternalStreamAsUpIterator : public UpIterator {
 public:
  explicit ExternalStreamAsUpIterator(const std::shared_ptr<ExternalStream>& es)
      : es_(es) {}

  bool hasNext() override {
    return es_->hasNext();
  }

  RowVectorPtr next() override {
    return es_->next();
  }

 private:
  std::shared_ptr<ExternalStream> es_;
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

  cacheMethod(env, "sessionId", kTypeLong, nullptr);
  addNativeMethod(
      "createMemoryManager",
      (void*)createMemoryManager,
      kTypeLong,
      "io/github/zhztheplayer/velox4j/memory/AllocationListener",
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
      "executeQuery", (void*)executeQuery, kTypeLong, kTypeString, nullptr);
  addNativeMethod(
      "upIteratorHasNext",
      (void*)upIteratorHasNext,
      kTypeBool,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "upIteratorNext", (void*)upIteratorNext, kTypeLong, kTypeLong, nullptr);
  addNativeMethod(
      "downIteratorAsExternalStream",
      (void*)downIteratorAsExternalStream,
      kTypeLong,
      "io/github/zhztheplayer/velox4j/iterator/DownIterator",
      nullptr);
  addNativeMethod(
      "variantInferType",
      (void*)variantInferType,
      kTypeString,
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
      "baseVectorDeserialize",
      (void*)baseVectorDeserialize,
      kTypeArray(kTypeLong),
      kTypeString,
      nullptr);
  addNativeMethod(
      "baseVectorGetType",
      (void*)baseVectorGetType,
      kTypeString,
      kTypeLong,
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
      "baseVectorGetEncoding",
      (void*)baseVectorGetEncoding,
      kTypeString,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "baseVectorNewRef",
      (void*)baseVectorNewRef,
      kTypeLong,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "deserializeAndSerialize",
      (void*)deserializeAndSerialize,
      kTypeString,
      kTypeString,
      nullptr);
  addNativeMethod(
      "deserializeAndSerializeVariant",
      (void*)deserializeAndSerializeVariant,
      kTypeString,
      kTypeString,
      nullptr);
  addNativeMethod(
      "createUpIteratorWithExternalStream",
      (void*)createUpIteratorWithExternalStream,
      kTypeLong,
      kTypeLong,
      nullptr);

  registerNativeMethods(env);
}

} // namespace velox4j
