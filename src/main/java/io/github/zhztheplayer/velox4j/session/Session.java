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
package io.github.zhztheplayer.velox4j.session;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.data.BaseVectors;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.data.SelectivityVectors;
import io.github.zhztheplayer.velox4j.eval.Evaluations;
import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.query.Queries;
import io.github.zhztheplayer.velox4j.serializable.ISerializables;
import io.github.zhztheplayer.velox4j.variant.Variants;
import io.github.zhztheplayer.velox4j.write.TableWriteTraits;

/**
 * A Velox4J session consists of a set of active Velox4J APIs.
 *
 * <p>Session itself should be closed after use, as it's a CppObject. Once it is closed, all the
 * created C++ objects will be destroyed to avoid memory leakage.
 */
public interface Session extends CppObject {

  /** APIs in relation to {@link io.github.zhztheplayer.velox4j.eval.Evaluation}. */
  Evaluations evaluationOps();

  /** APIs in relation to {@link io.github.zhztheplayer.velox4j.query.Query}. */
  Queries queryOps();

  /** APIs in relation to {@link io.github.zhztheplayer.velox4j.connector.ExternalStream}. */
  ExternalStreams externalStreamOps();

  /** APIs in relation to {@link io.github.zhztheplayer.velox4j.data.BaseVector}. */
  BaseVectors baseVectorOps();

  /** APIs in relation to {@link io.github.zhztheplayer.velox4j.data.RowVector}. */
  RowVectors rowVectorOps();

  /** APIs in relation to {@link io.github.zhztheplayer.velox4j.data.SelectivityVector}. */
  SelectivityVectors selectivityVectorOps();

  /**
   * Arrow APIs for vectors. This includes interchange functionalities between Velox native vector
   * format and Arrow-Java format.
   */
  Arrow arrowOps();

  /**
   * An API for creating certain required information for building a {@link
   * io.github.zhztheplayer.velox4j.plan.TableWriteNode} in Java.
   */
  TableWriteTraits tableWriteTraitsOps();

  /** APIs in relation to {@link io.github.zhztheplayer.velox4j.serializable.ISerializable}. */
  ISerializables iSerializableOps();

  /** APIs in relation to {@link io.github.zhztheplayer.velox4j.variant.Variant}. */
  Variants variantOps();
}
