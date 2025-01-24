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
#include <velox/common/memory/Memory.h>
#include "JniCommon.h"
#include "JniError.h"
#include "velox4j/exec/TaskRunner.h"
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

jlong executePlan(JNIEnv* env, jobject javaThis, jstring planJson) {
  JNI_METHOD_START
  spotify::jni::JavaString jPlanJson{env, planJson};
  TaskRunner runner{memory::memoryManager(), jPlanJson.get()};
  return sessionOf(env, javaThis)->objectStore()->save(runner.execute());
  JNI_METHOD_END(-1L)
}

jboolean upIteratorHasNext(JNIEnv* env, jobject javaThis, jlong itrId) {
  JNI_METHOD_START
  auto itr = velox4j::ObjectStore::retrieve<UpIterator>(itrId);
  return itr->hasNext();
  JNI_METHOD_END(false)
}

jlong upIteratorNext(JNIEnv* env, jobject javaThis, jlong itrId) {
  JNI_METHOD_START
  auto itr = velox4j::ObjectStore::retrieve<UpIterator>(itrId);
  return sessionOf(env, javaThis)->objectStore()->save(itr->next());
  JNI_METHOD_END(-1L)
}

void rowVectorExportToArrow(
    JNIEnv* env,
    jobject javaThis,
    jlong rvId,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  VELOX_NYI();
  JNI_METHOD_END()
}
} // namespace

void JniWrapper::mapFields() {}

const char* JniWrapper::getCanonicalName() const {
  return "io/github/zhztheplayer/velox4j/jni/JniWrapper";
}

void JniWrapper::initialize(JNIEnv* env) {
  JavaClass::setClass(env);

  VELOX_CHECK_NULL(mSessionId);
  std::string sSessionId;
  spotify::jni::JavaClassUtils::makeSignature(sSessionId, getCanonicalName(), kTypeLong, NULL);
  mSessionId = env->GetMethodID(_clazz, "sessionId", sSessionId.c_str());

  addNativeMethod("createSession", (void*)createSession, kTypeLong, NULL);
  addNativeMethod(
      "releaseCppObject", (void*)releaseCppObject, kTypeVoid, kTypeLong, NULL);
  addNativeMethod(
      "executePlan", (void*)executePlan, kTypeLong, kTypeString, NULL);
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

} // namespace velox4j
