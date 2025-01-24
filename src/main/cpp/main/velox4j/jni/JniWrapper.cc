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
#include "JniCommon.h"
#include "JniError.h"
#include "velox4j/lifecycle/ObjectStore.h"
#include "velox4j/exec/Executor.h"
#include <velox/common/memory/Memory.h>

namespace velox4j {
using namespace facebook::velox;

namespace {
ObjectStore* store() {
  // TODO: Make the object store session-wise to avoid leakages.
  static std::unique_ptr<ObjectStore> objectStore = ObjectStore::create();
  return objectStore.get();
}
}

void JniWrapper::mapFields() {}

const char* JniWrapper::getCanonicalName() const {
  return "io/github/zhztheplayer/velox4j/jni/JniWrapper";
}

void JniWrapper::initialize(JNIEnv* env) {
  JavaClass::setClass(env);

  addNativeMethod(
      "executePlan", (void*)executePlan, kTypeLong, kTypeString, NULL);
  addNativeMethod(
      "closeCppObject", (void*)closeCppObject, kTypeVoid, kTypeLong, NULL);
  addNativeMethod(
      "upIteratorHasNext",
      (void*)upIteratorHasNext,
      kTypeBool,
      kTypeLong,
      NULL);
  addNativeMethod(
      "upIteratorNext", (void*)upIteratorNext, kTypeLong, kTypeLong, NULL);
  addNativeMethod(
      "rowVectorExportToArrow",
      (void*)rowVectorExportToArrow,
      kTypeVoid,
      kTypeLong,
      kTypeLong,
      kTypeLong,
      NULL);

  registerNativeMethods(env);
}

jlong JniWrapper::executePlan(JNIEnv* env, jobject javaThis, jstring planJson) {
  JNI_METHOD_START
  spotify::jni::JavaString jPlanJson{env, planJson};
  Executor exec{memory::memoryManager(), jPlanJson.get()};
  return store()->save(exec.execute());
  JNI_METHOD_END(-1L)
}

void JniWrapper::closeCppObject(JNIEnv* env, jobject javaThis, jlong address){
    JNI_METHOD_START JNI_METHOD_END()}

jboolean JniWrapper::upIteratorHasNext(
    JNIEnv* env,
    jobject javaThis,
    jlong address) {
  JNI_METHOD_START
  return 0;
  JNI_METHOD_END(false)
}

jlong JniWrapper::upIteratorNext(JNIEnv* env, jobject javaThis, jlong address) {
  JNI_METHOD_START
  return 0;
  JNI_METHOD_END(-1L)
}

void JniWrapper::rowVectorExportToArrow(
    JNIEnv* env,
    jobject javaThis,
    jlong rvAddress,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  JNI_METHOD_END()
}

} // namespace velox4j
