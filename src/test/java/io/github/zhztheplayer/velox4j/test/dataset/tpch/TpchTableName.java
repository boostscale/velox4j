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
package io.github.zhztheplayer.velox4j.test.dataset.tpch;

import com.google.common.collect.ImmutableList;

import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.DecimalType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.VarCharType;

public enum TpchTableName {
  REGION(
      "region/region.parquet",
      new RowType(
          ImmutableList.of("r_regionkey", "r_name", "r_comment"),
          ImmutableList.of(new BigIntType(), new VarCharType(), new VarCharType()))),

  CUSTOMER(
      "customer/customer.parquet",
      new RowType(
          ImmutableList.of(
              "s_suppkey",
              "s_name",
              "s_address",
              "s_nationkey",
              "s_phone",
              "s_acctbal",
              "s_comment"),
          ImmutableList.of(
              new BigIntType(),
              new VarCharType(),
              new VarCharType(),
              new BigIntType(),
              new VarCharType(),
              new DecimalType(12, 2),
              new VarCharType()))),

  NATION(
      "nation/nation.parquet",
      new RowType(
          ImmutableList.of("n_nationkey", "n_name", "n_regionkey", "n_comment"),
          ImmutableList.of(
              new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType())));

  private final String relativePath;
  private final RowType schema;

  TpchTableName(String relativePath, RowType schema) {
    this.relativePath = relativePath;
    this.schema = schema;
  }

  public RowType schema() {
    return schema;
  }

  public String relativePath() {
    return relativePath;
  }
}
