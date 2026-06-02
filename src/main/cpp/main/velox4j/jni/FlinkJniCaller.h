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

#pragma once

#include <jni.h>
#include "JniCommon.h"
#include <velox/experimental/stateful/StatefulOperator.h>

namespace velox4j {
class FlinkJniCaller : public facebook::velox::stateful::JniCaller {
  public:
   void call(const std::string& clazzName, const std::string& methodName, const std::string& arg) override {
     JNIEnv* env = getLocalJNIEnv();
     if (env == nullptr) {
       VELOX_FAIL("Failed to get JNIEnv");
     }
     jclass clazz = env->FindClass(clazzName.c_str());
     if (clazz == nullptr) {
       VELOX_FAIL("Failed to find class {}", clazzName);
     }
     jmethodID methodId = env->GetStaticMethodID(clazz, methodName.c_str(), "(Ljava/lang/String;)V");
     if (methodId == nullptr) {
       env->DeleteLocalRef(clazz);
       VELOX_FAIL("Failed to find method {} in class {}", methodName, clazzName);
     }
     jstring argString = env->NewStringUTF(arg.c_str());
     if (argString == nullptr) {
       env->DeleteLocalRef(clazz);
       VELOX_FAIL("Failed to create string from arg {}", arg);
     }
     env->CallStaticVoidMethod(clazz, methodId, argString);
     if (env->ExceptionCheck()) {
       env->DeleteLocalRef(argString);
       env->DeleteLocalRef(clazz);
       VELOX_FAIL("Failed to call method {}", methodName);
     }
     env->DeleteLocalRef(argString);
     env->DeleteLocalRef(clazz);
   }
 };
} // namespace velox4j
