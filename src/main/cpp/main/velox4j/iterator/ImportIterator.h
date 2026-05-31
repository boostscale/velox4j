/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <JniHelpers.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <velox/vector/ComplexVector.h>
#include "velox4j/connector/ExternalStream.h"

namespace velox4j {
// JNI wrapper that exposes an import-iterator to Java.
class ImportIteratorJniWrapper final : public spotify::jni::JavaClass {
 public:
  explicit ImportIteratorJniWrapper(JNIEnv* env) : JavaClass(env) {
    ImportIteratorJniWrapper::initialize(env);
  }

  ImportIteratorJniWrapper() : JavaClass() {};

  const char* getCanonicalName() const override;

  void initialize(JNIEnv* env) override;

  void mapFields() override;
};

/// An ExternalStream that is backed by an import-iterator.
/// What is import-iterator: An import-iterator is an iterator passed
/// From Java to C++ for Velox to read data from Java.
/// An instance of this class is operating on a Java-side import-iterator
/// through JNI.
class ImportIterator : public ExternalStream {
 public:
  enum class State { AVAILABLE = 0, BLOCKED = 1, FINISHED = 2 };

  // CTOR.
  ImportIterator(JNIEnv* env, jobject ref);

  // Delete copy/move CTORs.
  ImportIterator(ImportIterator&&) = delete;
  ImportIterator(const ImportIterator&) = delete;
  ImportIterator& operator=(const ImportIterator&) = delete;
  ImportIterator& operator=(ImportIterator&&) = delete;

  // DTOR.
  ~ImportIterator() override;

  std::optional<facebook::velox::RowVectorPtr> read(
      facebook::velox::ContinueFuture& future) override;

 private:
  // Gets the next state.
  State advance();

  // Called once `advance` returns `BLOCKED` state to wait until
  // the state gets refreshed, either by the next row-vector
  // is ready for reading or by end of stream.
  void wait();

  // Called once `advance` returns `AVAILABLE` state to get
  // the next row-vector from the stream.
  facebook::velox::RowVectorPtr get();

  // Called to close the iterator.
  void close();

  jobject ref_;
  std::mutex mutex_;
  std::unique_ptr<folly::IOThreadPoolExecutor> waitExecutor_;
  std::vector<facebook::velox::ContinuePromise> promises_{};
  std::atomic_bool closed_{false};
};

} // namespace velox4j
