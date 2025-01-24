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

#include "Arrow.h"
#include <velox/vector/arrow/Bridge.h>

namespace velox4j {
using namespace facebook::velox;

namespace {
void flatten(const RowVectorPtr& in) {
  for (auto& child : in->children()) {
    facebook::velox::BaseVector::flattenVector(child);
    if (child->isLazy()) {
      child = child->as<facebook::velox::LazyVector>()->loadedVectorShared();
      VELOX_DCHECK_NOT_NULL(child);
    }
    // In case of output from Limit, RowVector size can be smaller than its
    // children size.
    if (child->size() > in->size()) {
      child = child->slice(0, in->size());
    }
  }
}
} // namespace

void exportRowVectorAsArrow(
    const RowVectorPtr& rv,
    ArrowSchema* cSchema,
    ArrowArray* cArray) {
  ArrowOptions options;
  options.timestampUnit = static_cast<TimestampUnit>(6);
  flatten(rv);
  exportToArrow(rv, *cSchema, options);
  exportToArrow(rv, *cArray, rv->pool(), options);
}
} // namespace velox4j
