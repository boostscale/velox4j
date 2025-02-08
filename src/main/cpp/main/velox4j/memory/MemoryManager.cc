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

#include "MemoryManager.h"
#include "ArrowMemoryPool.h"

namespace velox4j {
using namespace facebook;

namespace {

constexpr std::string_view kMemoryPoolInitialCapacity{
    "memory-pool-initial-capacity"};
constexpr uint64_t kDefaultMemoryPoolInitialCapacity{256 << 20};
constexpr std::string_view kMemoryPoolTransferCapacity{
    "memory-pool-transfer-capacity"};
constexpr uint64_t kDefaultMemoryPoolTransferCapacity{128 << 20};
constexpr std::string_view kMemoryReclaimMaxWaitMs{
    "memory-reclaim-max-wait-time"};
constexpr std::string_view kDefaultMemoryReclaimMaxWaitMs{"3600000ms"};

template <typename T>
T getConfig(
    const std::unordered_map<std::string, std::string>& configs,
    const std::string_view& key,
    const T& defaultValue) {
  if (configs.count(std::string(key)) > 0) {
    try {
      return folly::to<T>(configs.at(std::string(key)));
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(
          "Failed while parsing SharedArbitrator configs: {}", e.what());
    }
  }
  return defaultValue;
}

class ListenableArbitrator : public velox::memory::MemoryArbitrator {
 public:
  ListenableArbitrator(const Config& config, AllocationListener* listener)
      : MemoryArbitrator(config),
        listener_(listener),
        memoryPoolInitialCapacity_(velox::config::toCapacity(
            getConfig<std::string>(
                config.extraConfigs,
                kMemoryPoolInitialCapacity,
                std::to_string(kDefaultMemoryPoolInitialCapacity)),
            velox::config::CapacityUnit::BYTE)),
        memoryPoolTransferCapacity_(velox::config::toCapacity(
            getConfig<std::string>(
                config.extraConfigs,
                kMemoryPoolTransferCapacity,
                std::to_string(kDefaultMemoryPoolTransferCapacity)),
            velox::config::CapacityUnit::BYTE)),
        memoryReclaimMaxWaitMs_(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                velox::config::toDuration(getConfig<std::string>(
                    config.extraConfigs,
                    kMemoryReclaimMaxWaitMs,
                    std::string(kDefaultMemoryReclaimMaxWaitMs))))
                .count()) {}
  std::string kind() const override {
    return kind_;
  }

  void shutdown() override {}

  void addPool(
      const std::shared_ptr<velox::memory::MemoryPool>& pool) override {
    VELOX_CHECK_EQ(pool->capacity(), 0);

    std::unique_lock guard{mutex_};
    VELOX_CHECK_EQ(candidates_.count(pool.get()), 0);
    candidates_.emplace(pool.get(), pool->weak_from_this());
  }

  void removePool(velox::memory::MemoryPool* pool) override {
    VELOX_CHECK_EQ(pool->reservedBytes(), 0);
    shrinkCapacity(pool, pool->capacity());

    std::unique_lock guard{mutex_};
    const auto ret = candidates_.erase(pool);
    VELOX_CHECK_EQ(ret, 1);
  }

  bool growCapacity(velox::memory::MemoryPool* pool, uint64_t targetBytes)
      override {
    // Set arbitration context to allow memory over-use during recursive
    // arbitration. See MemoryPoolImpl::maybeIncrementReservation.
    velox::memory::ScopedMemoryArbitrationContext ctx{};
    velox::memory::MemoryPool* candidate;
    {
      std::unique_lock guard{mutex_};
      VELOX_CHECK_EQ(
          candidates_.size(),
          1,
          "ListenableArbitrator should only be used within a single root pool");
      candidate = candidates_.begin()->first;
    }
    VELOX_CHECK(
        pool->root() == candidate, "Illegal state in ListenableArbitrator");

    growCapacityInternal(pool->root(), targetBytes);
    return true;
  }

  uint64_t shrinkCapacity(
      uint64_t targetBytes,
      bool allowSpill,
      bool allowAbort) override {
    velox::memory::ScopedMemoryArbitrationContext ctx{};
    velox::memory::MemoryReclaimer::Stats status;
    velox::memory::MemoryPool* pool = nullptr;
    {
      std::unique_lock guard{mutex_};
      VELOX_CHECK_EQ(
          candidates_.size(),
          1,
          "ListenableArbitrator should only be used within a single root pool");
      pool = candidates_.begin()->first;
    }
    pool->reclaim(
        targetBytes, memoryReclaimMaxWaitMs_, status); // ignore the output
    return shrinkCapacityInternal(pool, 0);
  }

  uint64_t shrinkCapacity(velox::memory::MemoryPool* pool, uint64_t targetBytes)
      override {
    return shrinkCapacityInternal(pool, targetBytes);
  }

  Stats stats() const override {
    Stats stats; // no-op
    return stats;
  }

  std::string toString() const override {
    return fmt::format(
        "ARBITRATOR[{}] CAPACITY {} {}",
        kind_,
        velox::succinctBytes(capacity_),
        stats().toString());
  }

 private:
  void growCapacityInternal(velox::memory::MemoryPool* pool, uint64_t bytes) {
    // Since
    // https://github.com/facebookincubator/velox/pull/9557/files#diff-436e44b7374032f8f5d7eb45869602add6f955162daa2798d01cc82f8725724dL812-L820,
    // We should pass bytes as parameter "reservationBytes" when calling ::grow.
    auto freeByes = pool->freeBytes();
    if (freeByes > bytes) {
      if (growPool(pool, 0, bytes)) {
        return;
      }
    }
    auto reclaimedFreeBytes = shrinkPool(pool, 0);
    auto neededBytes = velox::bits::roundUp(
        bytes - reclaimedFreeBytes, memoryPoolTransferCapacity_);
    listener_->allocationChanged(neededBytes);
    auto ret = growPool(pool, reclaimedFreeBytes + neededBytes, bytes);
    VELOX_CHECK(
        ret,
        "{} failed to grow {} bytes, current state {}",
        pool->name(),
        velox::succinctBytes(bytes),
        pool->toString());
  }

  uint64_t shrinkCapacityInternal(
      velox::memory::MemoryPool* pool,
      uint64_t bytes) {
    uint64_t freeBytes = shrinkPool(pool, bytes);
    listener_->allocationChanged(-freeBytes);
    return freeBytes;
  }

  const uint64_t memoryPoolInitialCapacity_; // FIXME: Unused.
  const uint64_t memoryPoolTransferCapacity_;
  const uint64_t memoryReclaimMaxWaitMs_;
  AllocationListener* listener_{nullptr};

  mutable std::mutex mutex_;
  inline static std::string kind_ = "VELOX4J";
  std::unordered_map<
      velox::memory::MemoryPool*,
      std::weak_ptr<velox::memory::MemoryPool>>
      candidates_;
};

class ArbitratorFactoryRegister {
 public:
  explicit ArbitratorFactoryRegister(AllocationListener* listener)
      : listener_(listener) {
    static std::atomic_uint32_t id{0UL};
    kind_ = "VELOX4J_ARBITRATOR_FACTORY_" + std::to_string(id++);
    velox::memory::MemoryArbitrator::registerFactory(
        kind_,
        [this](const velox::memory::MemoryArbitrator::Config& config)
            -> std::unique_ptr<velox::memory::MemoryArbitrator> {
          return std::make_unique<ListenableArbitrator>(config, listener_);
        });
  }

  virtual ~ArbitratorFactoryRegister() {
    velox::memory::MemoryArbitrator::unregisterFactory(kind_);
  }

  const std::string& getKind() const {
    return kind_;
  }

 private:
  std::string kind_;
  AllocationListener* listener_;
};
} // namespace

MemoryManager::MemoryManager(std::unique_ptr<AllocationListener> listener)
    : listener_(std::move(listener)) {
  arrowAllocator_ = std::make_unique<ListenableMemoryAllocator>(
      defaultMemoryAllocator().get(), listener_.get());
  std::unordered_map<std::string, std::string> extraArbitratorConfigs;
  ArbitratorFactoryRegister afr(listener_.get());
  velox::memory::MemoryManagerOptions mmOptions{
      .alignment = velox::memory::MemoryAllocator::kMaxAlignment,
      .trackDefaultUsage = true, // memory usage tracking
      .checkUsageLeak = true, // leak check
      .debugEnabled = false, // debug
      .coreOnAllocationFailureEnabled = false,
      .allocatorCapacity = velox::memory::kMaxMemory,
      .arbitratorKind = afr.getKind(),
      .extraArbitratorConfigs = extraArbitratorConfigs};
  veloxMemoryManager_ =
      std::make_unique<velox::memory::MemoryManager>(mmOptions);
}

velox::memory::MemoryPool* MemoryManager::getVeloxPool(
    const std::string& name,
    const velox::memory::MemoryPool::Kind& kind) {
  if (veloxPoolRefs_.count(name) > 0) {
    const auto& pool = veloxPoolRefs_[name];
    VELOX_CHECK_EQ(
        pool->kind(),
        kind,
        "Pool [" + name + "] was already created but with different kind");
    return pool.get();
  }
  switch (kind) {
    case velox::memory::MemoryPool::Kind::kLeaf: {
      auto pool = veloxMemoryManager_->addLeafPool(name, true);
      veloxPoolRefs_[name] = pool;
      return pool.get();
    }
    case velox::memory::MemoryPool::Kind::kAggregate: {
      auto pool = veloxMemoryManager_->addRootPool(
          name,
          velox::memory::kMaxMemory,
          velox::memory::MemoryReclaimer::create());
      veloxPoolRefs_[name] = pool;
      return pool.get();
    }
  }
}

arrow::MemoryPool* MemoryManager::getArrowPool(const std::string& name) {
  if (arrowPoolRefs_.count(name) > 0) {
    return arrowPoolRefs_[name].get();
  }
  arrowPoolRefs_[name] =
      std::make_unique<ArrowMemoryPool>(arrowAllocator_.get());
  return arrowPoolRefs_[name].get();
}


namespace {
void hold0(
    std::vector<std::shared_ptr<velox::memory::MemoryPool>>& container,
    const velox::memory::MemoryPool* pool) {
  pool->visitChildren([&](velox::memory::MemoryPool* child) -> bool {
    auto shared = child->shared_from_this();
    container.push_back(shared);
    hold0(container, child);
    return true;
  });
}
} // namespace

void MemoryManager::holdPools() {
  for (const auto& pool : veloxPoolRefs_) {
    hold0(heldVeloxPoolRefs_, pool.second.get());
  }
}
} // namespace velox4j
