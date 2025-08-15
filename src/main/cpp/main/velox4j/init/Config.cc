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

#include "velox4j/init/Config.h"

namespace velox4j {
using namespace facebook::velox;

/// The preset used for initializing Velox4J.
/// The initialization process will involve connector, function, fs
/// registrations and so on.
/// Spark is the only preset we support at this moment.
config::ConfigBase::Entry<Preset> VELOX4J_INIT_PRESET(
    "velox4j.init.preset",
    Preset::SPARK);

/// The glog verbose level.
config::ConfigBase::Entry<int32_t> VELOX4J_INIT_LOGGING_VERBOSE_LEVEL(
    "velox4j.init.logging.verboseLevel",
    0);
} // namespace velox4j
