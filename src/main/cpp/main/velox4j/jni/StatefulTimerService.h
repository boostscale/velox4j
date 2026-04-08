// /*
//  * Licensed to the Apache Software Foundation (ASF) under one or more
//  * contributor license agreements.  See the NOTICE file distributed with
//  * this work for additional information regarding copyright ownership.
//  * The ASF licenses this file to You under the Apache License, Version 2.0
//  * (the "License"); you may not use this file except in compliance with
//  * the License.  You may obtain a copy of the License at
//  *
//  *    http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

// #pragma once

// #include <cstdint>
// #include <string>
// #include <unordered_map>
// #include <map>
// #include <folly/json.h>
// #include <folly/json/dynamic.h>
// #include <folly/json/json.h>
// #include <velox/experimental/stateful/InternalTimerService.h>
// #include "JniCommon.h"

// namespace velox4j {

// // 使用 Folly 将 timers 转为 JSON 字符串，格式: {"key1":{"name1":123,"name2":456},"key2":{...}}
// template <typename K, typename N>
// std::string timersToJsonString(
//     const std::unordered_map<K, std::map<N, int64_t>>& timers) {
//   folly::dynamic root = folly::dynamic::object;
//   for (const auto& kv : timers) {
//     folly::dynamic inner = folly::dynamic::object;
//     for (const auto& nv : kv.second) {
//       inner[folly::to<std::string>(nv.first)] = nv.second;
//     }
//     root[folly::to<std::string>(kv.first)] = std::move(inner);
//   }
//   return folly::toJson(root);
// }

// template<typename K, typename N>
// class StatefulTimerService : public facebook::velox::stateful::InternalTimerService<K, N> {
//  public:
//   StatefulTimerService(const std::string& serviceId)
//       : serviceId_(serviceId), operator_(nullptr), registerTimerMethodId_(nullptr) {}
  
//   void registerProcessingTimeTimers(const std::unordered_map<K, std::map<N, int64_t>>& timers) override {
//     // 必须用 GlobalRef 保存 operator_：local ref 不能跨线程；首次可能在 A 线程 init，后续在 timer 线程调用
//     if (operator_ == nullptr) {
//       init("registerProcessingTimeTimers");
//     }
//     JNIEnv* env = getLocalJNIEnv();
//     if (env == nullptr || operator_ == nullptr || registerTimerMethodId_ == nullptr) {
//       return;
//     }
//     std::string jsonString = timersToJsonString(timers);
//     jstring jsonStringJ = env->NewStringUTF(jsonString.c_str());
//     if (jsonStringJ == nullptr) {
//       env->ExceptionClear();
//       return;
//     }
//     env->CallVoidMethod(operator_, registerTimerMethodId_, jsonStringJ);
//     env->DeleteLocalRef(jsonStringJ);
//     if (env->ExceptionCheck()) {
//       env->ExceptionDescribe();
//       env->ExceptionClear();
//     }
//   }
//   void registerEventTimeTimers(const std::unordered_map<K, std::map<N, int64_t>>& timers) override {
//     if (operator_ == nullptr) {
//       init("registerEventTimeTimers");
//     }
//     JNIEnv* env = getLocalJNIEnv();
//     if (env == nullptr || operator_ == nullptr || registerTimerMethodId_ == nullptr) {
//       return;
//     }
//     std::string jsonString = timersToJsonString(timers);
//     jstring jsonStringJ = env->NewStringUTF(jsonString.c_str());
//     if (jsonStringJ == nullptr) {
//       env->ExceptionClear();
//       return;
//     }
//     env->CallVoidMethod(operator_, registerTimerMethodId_, jsonStringJ);
//     env->DeleteLocalRef(jsonStringJ);
//     if (env->ExceptionCheck()) {
//       env->ExceptionDescribe();
//       env->ExceptionClear();
//     }
//   }
//   void deleteProcessingTimeTimers(const std::unordered_map<K, std::map<N, int64_t>>& timers) override {
//     // TODO: Implement deleteProcessingTimeTimers logic.
//   }
//   void close() override {
//     if (operator_ != nullptr) {
//       JNIEnv* env = getLocalJNIEnv();
//       if (env != nullptr) {
//         env->DeleteGlobalRef(operator_);
//       }
//       operator_ = nullptr;
//     }
//   }
//  private:
//   std::string serviceId_;
//   JNIEnv* env_;  // 仅用于 init()
//   jobject operator_;  // 必须为 NewGlobalRef，供任意线程的 JNI 调用使用
//   jmethodID registerTimerMethodId_;

//   void init(const std::string& methodName) {
//     operator_ = nullptr;
//     env_ = getLocalJNIEnv();
//     if (env_ == nullptr) return;
//     jclass clazz = env_->FindClass("org/apache/gluten/table/runtime/operators/GlutenSessionResources");

//     // 2. 获取静态方法ID
//     jmethodID getSessionResourcesMethodId = env_->GetStaticMethodID(
//       clazz,
//       "getInstance",
//       "()Lorg/apache/gluten/table/runtime/operators/GlutenSessionResources;");
//     if (getSessionResourcesMethodId == nullptr) {
//       env_->ExceptionClear();
//       env_->DeleteLocalRef(clazz);
//       return;
//     }
//     // 3. 调用静态方法
//     jobject sessionResources = env_->CallStaticObjectMethod(clazz, getSessionResourcesMethodId);
//     // 4. 处理异常
//     if (env_->ExceptionCheck()) {
//       env_->ExceptionDescribe();
//       env_->ExceptionClear();
//       env_->DeleteLocalRef(clazz);
//       return;
//     }
//     if (sessionResources == nullptr) {
//       LOG(WARNING) << "GlutenSessionResources.getInstance() returned null.";
//       env_->DeleteLocalRef(clazz);
//       return;
//     }
//     // getOperator(String) 返回 GlutenOperator —— 必须是实例方法；若为 static 须改用 GetStaticMethodID + CallStaticObjectMethod
//     jmethodID getOperatorMethodId = env_->GetMethodID(
//         clazz,
//         "getOperator",
//         "(Ljava/lang/String;)Lorg/apache/gluten/streaming/api/operators/GlutenOperator;");
//     if (getOperatorMethodId == nullptr) {
//       env_->ExceptionClear();
//       env_->DeleteLocalRef(clazz);
//       env_->DeleteLocalRef(sessionResources);
//       return;
//     }
//     jstring operatorId = env_->NewStringUTF("window-agg-operator");
//     if (operatorId == nullptr) {
//       env_->ExceptionClear();
//       env_->DeleteLocalRef(clazz);
//       env_->DeleteLocalRef(sessionResources);
//       return;
//     }
//     jobject op = env_->CallObjectMethod(sessionResources, getOperatorMethodId, operatorId);
//     if (env_->ExceptionCheck()) {
//       env_->ExceptionDescribe();
//       env_->ExceptionClear();
//       env_->DeleteLocalRef(operatorId);
//       env_->DeleteLocalRef(clazz);
//       env_->DeleteLocalRef(sessionResources);
//       return;
//     }
//     // op 为 null 表示 Java getOperator("window-agg-operator") 返回了 null，即该 id 尚未注册
//     if (op == nullptr) {
//       LOG(WARNING) << "GlutenSessionResources.getOperator(\"window-agg-operator\") returned null. "
//                    << "Ensure the operator is registered (e.g. before StatefulTimerService is used).";
//       env_->DeleteLocalRef(operatorId);
//       env_->DeleteLocalRef(clazz);
//       env_->DeleteLocalRef(sessionResources);
//       return;
//     }
//     env_->DeleteLocalRef(clazz);
//     env_->DeleteLocalRef(sessionResources);

//     jclass operatorClazz = env_->FindClass("org/apache/gluten/streaming/api/operators/GlutenOperator");
//     jmethodID registerTimerMethodId = env_->GetMethodID(
//       operatorClazz, methodName.c_str(), "(Ljava/lang/String;)V");
//     if (registerTimerMethodId == nullptr) {
//       env_->ExceptionClear();
//       env_->DeleteLocalRef(operatorClazz);
//       env_->DeleteLocalRef(operatorId);
//       env_->DeleteLocalRef(op);
//       return;
//     }
//     operator_ = env_->NewGlobalRef(op);
//     env_->DeleteLocalRef(op);
//     if (operator_ == nullptr) {
//       LOG(ERROR) << "NewGlobalRef(operator) failed (OOM?).";
//       env_->DeleteLocalRef(operatorClazz);
//       env_->DeleteLocalRef(operatorId);
//       return;
//     }
//     registerTimerMethodId_ = registerTimerMethodId;
//     env_->DeleteLocalRef(operatorClazz);
//     env_->DeleteLocalRef(operatorId);
//   }
// };
// } // namespace velox4j