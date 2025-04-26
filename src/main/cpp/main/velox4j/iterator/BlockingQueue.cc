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

#include "BlockingQueue.h"

namespace velox4j {
using namespace facebook::velox;

BlockingQueue::BlockingQueue() {
  waitExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(1);
}

BlockingQueue::~BlockingQueue() {
  close();
}

std::optional<RowVectorPtr> BlockingQueue::read(
    ContinueFuture& future) {
  {
    std::unique_lock lock(mutex_);

    if (!queue_.empty()) {
      auto rowVector = queue_.front();
      queue_.pop();
      return rowVector;
    }
  }

  // Blocked. Async wait for a new element.
  auto [readPromise, readFuture] =
      makeVeloxContinuePromiseContract(
          fmt::format("BlockingQueue::read"));
  // Returns a future that is fulfilled immediately to signal Velox
  // that this stream is still open and is currently waiting for input.
  future = std::move(readFuture);
  {
    std::lock_guard l(mutex_);
    VELOX_CHECK(promises_.empty());
    promises_.emplace_back(std::move(readPromise));
  }

  waitExecutor_->add([this]() -> void {
    std::unique_lock lock(mutex_);
    // Async wait for a new element.
    condVar_.wait(lock, [this]() { return closed_ || !queue_.empty(); });
    if (closed_) {
      try {
        VELOX_FAIL("BlockingQueue was just closed");
      } catch (const std::exception& e) {
        // Velox should guarantee the continue future is only requested once
        // while it's not fulfilled.
        VELOX_CHECK(promises_.size() == 1);
        for (auto& p : promises_) {
          p.setException(e);
          promises_.clear();
          return;
        }
      }
    }
    VELOX_CHECK(!queue_.empty());
    VELOX_CHECK(promises_.size() == 1);
    for (auto& p : promises_) {
      p.setValue();
    }
    promises_.clear();
  });

  return std::nullopt;
}

void BlockingQueue::put(RowVectorPtr rowVector) {
  {
    std::lock_guard lock(mutex_);
    queue_.push(std::move(rowVector));
  }
  condVar_.notify_one();
}

bool BlockingQueue::empty() const {
  {
    std::lock_guard lock(mutex_);
    return queue_.empty();
  }
}

void BlockingQueue::close() {
  {
    std::lock_guard lock(mutex_);
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true)) {
      return;
    }
  }
  condVar_.notify_one();
  waitExecutor_->join();
}
} // namespace velox4j
