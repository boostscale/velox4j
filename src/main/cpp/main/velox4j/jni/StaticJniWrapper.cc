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

#include "StaticJniWrapper.h"
#include <folly/json/json.h>
#include <velox/common/encode/Base64.h>
#include <velox/vector/VectorSaver.h>
#include "JniCommon.h"
#include "JniError.h"
#include "velox4j/arrow/Arrow.h"
#include "velox4j/config/Config.h"
#include "velox4j/init/Init.h"
#include "velox4j/iterator/UpIterator.h"
#include "velox4j/lifecycle/Session.h"
#include "velox4j/memory/JavaAllocationListener.h"

namespace velox4j {
using namespace facebook::velox;

namespace {
const char* kClassName = "io/github/zhztheplayer/velox4j/jni/StaticJniWrapper";

void initialize0(JNIEnv* env, jobject javaThis, jstring globalConfJson) {
  JNI_METHOD_START
  spotify::jni::JavaString jGlobalConfJson{env, globalConfJson};
  auto dynamic = folly::parseJson(jGlobalConfJson.get());
  auto confArray = ConfigArray::create(dynamic);
  initialize(confArray);
  JNI_METHOD_END()
}

jlong createMemoryManager(JNIEnv* env, jobject javaThis, jobject jListener) {
  JNI_METHOD_START
  auto listener = std::make_unique<BlockAllocationListener>(
      std::make_unique<JavaAllocationListener>(env, jListener), 8 << 10 << 10);
  auto mm = std::make_shared<MemoryManager>(std::move(listener));
  return ObjectStore::global()->save(mm);
  JNI_METHOD_END(-1L)
}

jlong createSession(JNIEnv* env, jobject javaThis, long memoryManagerId) {
  JNI_METHOD_START
  auto mm = ObjectStore::retrieve<MemoryManager>(memoryManagerId);
  return ObjectStore::global()->save(std::make_unique<Session>(mm.get()));
  JNI_METHOD_END(-1L)
}

void releaseCppObject(JNIEnv* env, jobject javaThis, jlong objId) {
  JNI_METHOD_START
  ObjectStore::release(objId);
  JNI_METHOD_END()
}

jboolean upIteratorHasNext(JNIEnv* env, jobject javaThis, jlong itrId) {
  JNI_METHOD_START
  auto itr = ObjectStore::retrieve<UpIterator>(itrId);
  return itr->hasNext();
  JNI_METHOD_END(false)
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

jstring baseVectorGetType(JNIEnv* env, jobject javaThis, jlong vid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  auto serializedDynamic = vector->type()->serialize();
  auto serializeJson = folly::toPrettyJson(serializedDynamic);
  return env->NewStringUTF(serializeJson.data());
  JNI_METHOD_END(nullptr)
}

jstring baseVectorGetEncoding(JNIEnv* env, jobject javaThis, jlong vid) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<BaseVector>(vid);
  auto name = VectorEncoding::mapSimpleToName(vector->encoding());
  return env->NewStringUTF(name.data());
  JNI_METHOD_END(nullptr)
}

jboolean selectivityVectorIsValid(JNIEnv* env, jobject javaThis, jlong svId, jint idx) {
  JNI_METHOD_START
  auto vector = ObjectStore::retrieve<SelectivityVector>(svId);
  auto valid = vector->isValid(static_cast<vector_size_t>(idx));
  return static_cast<jboolean>(valid);
  JNI_METHOD_END(false)
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
} // namespace

const char* StaticJniWrapper::getCanonicalName() const {
  return kClassName;
}

void StaticJniWrapper::initialize(JNIEnv* env) {
  JavaClass::setClass(env);

  addNativeMethod(
      "initialize", (void*)initialize0, kTypeVoid, kTypeString, nullptr);
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
      "upIteratorHasNext",
      (void*)upIteratorHasNext,
      kTypeBool,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "variantInferType",
      (void*)variantInferType,
      kTypeString,
      kTypeString,
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
      "baseVectorGetType",
      (void*)baseVectorGetType,
      kTypeString,
      kTypeLong,
      nullptr);
  addNativeMethod(
      "baseVectorGetEncoding",
      (void*)baseVectorGetEncoding,
      kTypeString,
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
      "deserializeAndSerializeVariant",
      (void*)deserializeAndSerializeVariant,
      kTypeString,
      kTypeString,
      nullptr);

  registerNativeMethods(env);
}

void StaticJniWrapper::mapFields() {}
} // namespace velox4j
