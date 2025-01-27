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
#include "velox4j/exec/QueryExecutor.h"
#include "velox4j/lifecycle/Session.h"

namespace velox4j {
using namespace facebook::velox;

namespace {
jmethodID mSessionId = nullptr;

long createSession(JNIEnv* env, jobject javaThis) {
  JNI_METHOD_START
  return ObjectStore::global()->save(std::make_unique<Session>());
  JNI_METHOD_END(-1L)
}

Session* sessionOf(JNIEnv* env, jobject javaThis) {
  jlong sessionId = env->CallLongMethod(javaThis, mSessionId);
  return ObjectStore::retrieve<Session>(sessionId).get();
}

void releaseCppObject(JNIEnv* env, jobject javaThis, jlong objId) {
  JNI_METHOD_START
  ObjectStore::release(objId);
  JNI_METHOD_END()
}

jlong executeQuery(JNIEnv* env, jobject javaThis, jstring queryJson) {
  JNI_METHOD_START
  spotify::jni::JavaString jQueryJson{env, queryJson};
  QueryExecutor exec{memory::memoryManager(), jQueryJson.get()};
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

jlong arrowToBaseVector(
    JNIEnv* env,
    jobject javaThis,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  // TODO Session memory pool.
  auto session = sessionOf(env, javaThis);
  static std::atomic<uint32_t> nextId{0}; // Velox query ID, same with taskId.
  const uint32_t id = nextId++;
  auto pool = memory::memoryManager()->addLeafPool(
      fmt::format("Arrow Import Memory Pool - ID {}", id));
  session->objectStore()->save(pool);
  auto vector = importArrowAsBaseVector(
      pool.get(),
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
  exportBaseVectorAsArrow(
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

  static std::atomic<uint32_t> nextId{0}; // Velox query ID, same with taskId.
  const uint32_t id = nextId++;
  auto pool = memory::memoryManager()->addLeafPool(
      fmt::format("Decoding Memory Pool - ID {}", id));
  session->objectStore()->save(pool);
  std::vector<ObjectHandle> vids{};
  while (dataStream.tellg() < decoded.size()) {
    const VectorPtr& vector = restoreVector(dataStream, pool.get());
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
  static std::atomic<uint32_t> nextId{0}; // Velox query ID, same with taskId.
  const uint32_t id = nextId++;
  auto serdePool = memory::memoryManager()->addLeafPool(
      fmt::format("Serde Memory Pool - ID {}", id));
  spotify::jni::JavaString jJson{env, json};
  auto dynamic = folly::parseJson(jJson.get());
  auto deserialized =
      ISerializable::deserialize<ISerializable>(dynamic, serdePool.get());
  auto serializedDynamic = deserialized->serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
  JNI_METHOD_END(nullptr)
}

jstring
deserializeAndSerializeVariant(JNIEnv* env, jobject javaThis, jstring json) {
  JNI_METHOD_START
  static std::atomic<uint32_t> nextId{0}; // Velox query ID, same with taskId.
  const uint32_t id = nextId++;
  auto serdePool = memory::memoryManager()->addLeafPool(
      fmt::format("Serde Memory Pool - ID {}", id));
  spotify::jni::JavaString jJson{env, json};
  auto dynamic = folly::parseJson(jJson.get());
  auto deserialized = variant::create(dynamic);
  auto serializedDynamic = deserialized.serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
  JNI_METHOD_END(nullptr)
}
} // namespace

void JniWrapper::mapFields() {}

const char* JniWrapper::getCanonicalName() const {
  return "io/github/zhztheplayer/velox4j/jni/JniWrapper";
}

void JniWrapper::initialize(JNIEnv* env) {
  JavaClass::setClass(env);

  {
    VELOX_CHECK_NULL(mSessionId);
    std::string signature;
    spotify::jni::JavaClassUtils::makeSignature(signature, kTypeLong, NULL);
    mSessionId = env->GetMethodID(_clazz, "sessionId", signature.c_str());
  }

  addNativeMethod("createSession", (void*)createSession, kTypeLong, NULL);
  addNativeMethod(
      "releaseCppObject", (void*)releaseCppObject, kTypeVoid, kTypeLong, NULL);
  addNativeMethod(
      "executeQuery", (void*)executeQuery, kTypeLong, kTypeString, NULL);
  addNativeMethod(
      "upIteratorHasNext",
      (void*)upIteratorHasNext,
      kTypeBool,
      kTypeLong,
      NULL);
  addNativeMethod(
      "upIteratorNext", (void*)upIteratorNext, kTypeLong, kTypeLong, NULL);
  addNativeMethod(
      "arrowToBaseVector",
      (void*)arrowToBaseVector,
      kTypeLong,
      kTypeLong,
      kTypeLong,
      NULL);
  addNativeMethod(
      "baseVectorToArrow",
      (void*)baseVectorToArrow,
      kTypeVoid,
      kTypeLong,
      kTypeLong,
      kTypeLong,
      NULL);
  addNativeMethod(
      "baseVectorSerialize",
      (void*)baseVectorSerialize,
      kTypeString,
      kTypeArray(kTypeLong),
      NULL);
  addNativeMethod(
      "baseVectorDeserialize",
      (void*)baseVectorDeserialize,
      kTypeArray(kTypeLong),
      kTypeString,
      NULL);
  addNativeMethod(
      "baseVectorGetType",
      (void*)baseVectorGetType,
      kTypeString,
      kTypeLong,
      NULL);
  addNativeMethod(
      "baseVectorWrapInConstant",
      (void*)baseVectorWrapInConstant,
      kTypeLong,
      kTypeLong,
      kTypeInt,
      kTypeInt,
      NULL);
  addNativeMethod(
      "baseVectorGetEncoding",
      (void*)baseVectorGetEncoding,
      kTypeString,
      kTypeLong,
      NULL);
  addNativeMethod(
      "baseVectorNewRef", (void*)baseVectorNewRef, kTypeLong, kTypeLong, NULL);
  addNativeMethod(
      "deserializeAndSerialize",
      (void*)deserializeAndSerialize,
      kTypeString,
      kTypeString,
      NULL);
  addNativeMethod(
      "deserializeAndSerializeVariant",
      (void*)deserializeAndSerializeVariant,
      kTypeString,
      kTypeString,
      NULL);

  registerNativeMethods(env);
}

} // namespace velox4j
