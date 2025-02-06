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

#include "DownIterator.h"
#include "velox4j/jni/JniCommon.h"
#include "velox4j/lifecycle/ObjectStore.h"

namespace velox4j {

namespace {
const char* kClassName = "io/github/zhztheplayer/velox4j/iterator/DownIterator";

JNIEnv* getLocalJNIEnv() {
  static std::atomic<uint32_t> nextThreadId{0};
  if (spotify::jni::JavaThreadUtils::getEnvForCurrentThread() == nullptr) {
    const std::string name =
        fmt::format("Velox4j Native Thread {}", nextThreadId++);
    spotify::jni::JavaThreadUtils::attachCurrentThreadAsDaemonToJVM(
        name.c_str());
  }
  JNIEnv* env = spotify::jni::JavaThreadUtils::getEnvForCurrentThread();
  VELOX_CHECK(env != nullptr);
  return env;
}
} // namespace

void DownIteratorJniWrapper::mapFields() {}

const char* DownIteratorJniWrapper::getCanonicalName() const {
  return kClassName;
}

void DownIteratorJniWrapper::initialize(JNIEnv* env) {
  JavaClass::setClass(env);

  cacheMethod(env, "hasNext", kTypeBool, nullptr);
  cacheMethod(env, "next", kTypeLong, nullptr);

  registerNativeMethods(env);
}

DownIterator::DownIterator(JNIEnv* env, jobject ref) {
  ref_ = env->NewGlobalRef(ref);
}

DownIterator::~DownIterator() {
  getLocalJNIEnv()->DeleteGlobalRef(ref_);
}

bool DownIterator::hasNext() {
  auto* env = getLocalJNIEnv();
  static const auto* clazz = jniClassRegistry()->get(kClassName);
  static jmethodID methodId = clazz->getMethod("hasNext");
  const jboolean hasNext = env->CallBooleanMethod(ref_, methodId);
  checkException(env);
  return hasNext;
}

RowVectorPtr DownIterator::next() {
  auto* env = getLocalJNIEnv();
  static const auto* clazz = jniClassRegistry()->get(kClassName);
  static jmethodID methodId = clazz->getMethod("next");
  const jlong rvId = env->CallLongMethod(ref_, methodId);
  checkException(env);
  return ObjectStore::retrieve<RowVector>(rvId);
}
} // namespace velox4j
