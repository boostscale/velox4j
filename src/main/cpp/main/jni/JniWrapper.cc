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

const char* velox4j::JniWrapper::getCanonicalName() const {
  return "io/github/zhztheplayer/velox4j/jni/JniWrapper";
}

void velox4j::JniWrapper::initialize(JNIEnv* env) {
  JavaClass::setClass(env);

  addNativeMethod(
      "executePlan", (void*)executePlan, kTypeLong, kTypeString, NULL);
  addNativeMethod(
      "closeCppObject", (void*)closeCppObject, kTypeLong, kTypeLong, NULL);
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
}

jlong velox4j::JniWrapper::executePlan(
    JNIEnv* env,
    jobject javaThis,
    jstring jsonPlan) {
  JNI_METHOD_START
  return 0;
  JNI_METHOD_END(-1L)
}

jlong velox4j::JniWrapper::closeCppObject(
    JNIEnv* env,
    jobject javaThis,
    jlong address) {
  JNI_METHOD_START
  return 0;
  JNI_METHOD_END(-1L)
}

jboolean velox4j::JniWrapper::upIteratorHasNext(
    JNIEnv* env,
    jobject javaThis,
    jlong address) {
  JNI_METHOD_START
  return 0;
  JNI_METHOD_END(false)
}

jlong velox4j::JniWrapper::upIteratorNext(
    JNIEnv* env,
    jobject javaThis,
    jlong address) {
  JNI_METHOD_START
  return 0;
  JNI_METHOD_END(-1L)
}

void velox4j::JniWrapper::rowVectorExportToArrow(
    JNIEnv* env,
    jobject javaThis,
    jlong rvAddress,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  JNI_METHOD_END()
}
