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

#include <velox/vector/BaseVector.h>

namespace velox4j {

/// A customized version of `BaseVector::flattenVector`,
/// mainly to workaround
/// https://github.com/facebookincubator/velox/issues/14492. We also truncate
/// all vectors at `targetSize`, because some Velox operations (E.g., Limit)
/// could result in a RowVector whose children have larger size than itself. So
/// we perform a slice to keep only the data that is in real use.
void flattenVector(facebook::velox::VectorPtr& vector, size_t targetSize);
} // namespace velox4j
