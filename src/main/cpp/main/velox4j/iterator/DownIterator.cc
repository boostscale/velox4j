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
    const std::string threadName =
        fmt::format("Velox4j Native Thread {}", nextThreadId++);
    std::vector<char> threadNameCStr(threadName.length() + 1);
    std::strcpy(threadNameCStr.data(), threadName.data());
    JavaVM* vm = spotify::jni::JavaThreadUtils::getJavaVM();
    JNIEnv* env{nullptr};
    JavaVMAttachArgs args;
    args.version = JAVA_VERSION;
    args.name = threadNameCStr.data();
    args.group = nullptr;
    const int result =
        vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env), &args);
    if (result != JNI_OK) {
      VELOX_FAIL("Failed to reattach current thread to JVM.");
    }
    return env;
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

DownIterator::DownIterator(JNIEnv* env, jobject ref) : ExternalStream() {
  ref_ = env->NewGlobalRef(ref);
}

DownIterator::~DownIterator() {
  try {
    getLocalJNIEnv()->DeleteGlobalRef(ref_);
  } catch (const std::exception& ex) {
    LOG(WARNING)
        << "Unable to destroy the global reference to the Java side down iterator: "
        << ex.what();
  }
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
