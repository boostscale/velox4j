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
#include "JniCommon.h"
#include "JniError.h"
#include "velox4j/config/Config.h"
#include "velox4j/init/Init.h"
#include "velox4j/lifecycle/Session.h"
#include "velox4j/memory/JavaAllocationListener.h"

namespace velox4j {
namespace {
const char* kClassName = "io/github/zhztheplayer/velox4j/jni/StaticJniWrapper";

void initialize0(JNIEnv* env, jobject javaThis, jstring globalConfJson) {
  JNI_METHOD_START
  spotify::jni::JavaString jGlobalConfJson{env, globalConfJson};
  auto dynamic = folly::parseJson(jGlobalConfJson.get());
  auto confArray = ConfigArray::createSimple(dynamic);
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

  registerNativeMethods(env);
}

void StaticJniWrapper::mapFields() {}
} // namespace velox4j
