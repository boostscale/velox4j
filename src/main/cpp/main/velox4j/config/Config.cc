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

#include "Config.h"

namespace velox4j {

std::unordered_map<std::string, std::string> ConfigArray::toMap() const {
  std::unordered_map<std::string, std::string> map(values_.size());
  for (const auto& kv : values_) {
    if (map.find(kv.first) != map.end()) {
      VELOX_FAIL("Duplicate key {} in config array", kv.first);
    }
    map.emplace(kv.first, kv.second);
  }
  return std::move(map);
}

folly::dynamic ConfigArray::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "Velox4jConfig";
  folly::dynamic values = folly::dynamic::array;
  for (const auto& kv : values_) {
    folly::dynamic kvObj = folly::dynamic::object;
    kvObj["key"] = kv.first;
    kvObj["value"] = kv.second;
    values.push_back(kvObj);
  }
  obj["values"] = values;
  return obj;
};

std::shared_ptr<ConfigArray> ConfigArray::create(
    const folly::dynamic& obj,
    void* context) {
  std::vector<std::pair<std::string, std::string>> values;
  for (const auto& kv : obj["values"]) {
    values.emplace_back(kv["key"].asString(), kv["value"].asString());
  }
  return std::make_shared<ConfigArray>(std::move(values));
}

void ConfigArray::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("Velox4jConfig", create);
}
} // namespace velox4j
