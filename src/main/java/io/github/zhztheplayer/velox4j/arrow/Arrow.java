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
package io.github.zhztheplayer.velox4j.arrow;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;

public class Arrow {
  private final JniApi jniApi;

  public Arrow(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public Schema toArrowSchema(BufferAllocator alloc, RowType type) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc)) {
      jniApi.typeToArrow(type, cSchema);
      final Schema schema = Data.importSchema(alloc, cSchema, null);
      return schema;
    }
  }

  public static RowType fromArrowSchema(BufferAllocator alloc, Schema schema) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc)) {
      Data.exportSchema(alloc, schema, null, cSchema);
      final RowType type = (RowType) StaticJniApi.get().arrowToRowType(cSchema);
      return type;
    }
  }

  public Field toArrowField(BufferAllocator alloc, Type type) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc)) {
      jniApi.typeToArrow(type, cSchema);
      final Field field = Data.importField(alloc, cSchema, null);
      return field;
    }
  }

  public static Type fromArrowField(BufferAllocator alloc, Field field) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc)) {
      Data.exportField(alloc, field, null, cSchema);
      final Type type = StaticJniApi.get().arrowToRowType(cSchema);
      return type;
    }
  }

  public static FieldVector toArrowVector(BufferAllocator alloc, BaseVector vector) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
      StaticJniApi.get().baseVectorToArrow(vector, cSchema, cArray);
      final FieldVector fv = Data.importVector(alloc, cArray, cSchema, null);
      return fv;
    }
  }

  public BaseVector fromArrowVector(BufferAllocator alloc, FieldVector arrowVector) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
      Data.exportVector(alloc, arrowVector, null, cArray, cSchema);
      final BaseVector imported = jniApi.arrowToBaseVector(cSchema, cArray);
      return imported;
    }
  }

  public static VectorSchemaRoot toArrowVectorSchemaRoot(BufferAllocator alloc, RowVector vector) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
      StaticJniApi.get().baseVectorToArrow(vector, cSchema, cArray);
      final VectorSchemaRoot vsr = Data.importVectorSchemaRoot(alloc, cArray, cSchema, null);
      return vsr;
    }
  }

  public RowVector fromVectorSchemaRoot(BufferAllocator alloc, VectorSchemaRoot vsr) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
      Data.exportVectorSchemaRoot(alloc, vsr, null, cArray, cSchema);
      final BaseVector imported = jniApi.arrowToBaseVector(cSchema, cArray);
      return imported.asRowVector();
    }
  }
}
