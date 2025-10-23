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

#include "velox4j/lifecycle/ObjectStore.h"
#include <glog/logging.h>

namespace velox4j {
ObjectStore* ObjectStore::global() {
  static std::unique_ptr<ObjectStore> globalStore = create();
  return globalStore.get();
}

std::unique_ptr<ObjectStore> ObjectStore::create() {
  static std::mutex mtx;
  std::lock_guard<std::mutex> lock(mtx);
  StoreHandle nextId = safeCast<StoreHandle>(stores().nextId());
  auto store = std::unique_ptr<ObjectStore>(new ObjectStore(nextId));
  StoreHandle storeId = safeCast<StoreHandle>(stores().insert(store.get()));
  VELOX_CHECK(storeId == nextId, "Store ID mismatched, this should not happen");
  return store;
}

ResourceMap<ObjectStore*>& ObjectStore::stores() {
  static ResourceMap<ObjectStore*> stores;
  return stores;
}

ObjectStore::~ObjectStore() {
  // destructing in reversed order (the last added object destructed first)
  while (!aliveObjects_.empty()) {
    std::shared_ptr<void> tempObj;
    {
      const std::lock_guard<std::mutex> lock(mtx_);
      // destructing in reversed order (the last added object destructed first)
      auto itr = aliveObjects_.rbegin();
      const ResourceHandle handle = (*itr).first;
      const std::string_view description = (*itr).second;
      VLOG(2)
          << "Unclosed object [" << "Store ID: " << storeId_
          << ", Resource handle ID: " << handle
          << ", Description: " << description
          << "] is found when object store is closing. Velox4J will"
             " destroy it automatically but it's recommended to manually close"
             " the object through the Java API CppObject#close() after use,"
             " to minimize peak memory pressure of the application.";
      tempObj = store_.lookup(handle);
      store_.erase(handle);
      aliveObjects_.erase(handle);
    }
    tempObj.reset(); // this will call the destructor of the object
  }
  stores().erase(storeId_);
}

void ObjectStore::releaseInternal(ResourceHandle handle) {
  const std::lock_guard<std::mutex> lock(mtx_);
  store_.erase(handle);
  aliveObjects_.erase(handle);
}
} // namespace velox4j
