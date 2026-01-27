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
package io.github.zhztheplayer.velox4j.connector;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.zhztheplayer.velox4j.serializable.ISerializable;

public class KafkaConnectorSplit extends ConnectorSplit {
  private String bootstrapServers;

  private String groupId;

  private String format;

  private boolean enableAutoCommit;

  private String autoResetoffset;

  private List<TopicPartitionOffset> topicPartitions;

  @JsonCreator
  public KafkaConnectorSplit(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("splitWeight") long splitWeight,
      @JsonProperty("cacheable") boolean cacheable,
      @JsonProperty("bootstrapServers") String bootstrapServers,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("format") String format,
      @JsonProperty("enableAutoCommit") boolean enableAutoCommit,
      @JsonProperty("autoResetOffset") String autoResetOffset,
      @JsonProperty("topicPartitions") List<TopicPartitionOffset> topicPartitions) {
    super(connectorId, splitWeight, cacheable);
    this.bootstrapServers = bootstrapServers;
    this.groupId = groupId;
    this.format = format;
    this.enableAutoCommit = enableAutoCommit;
    this.autoResetoffset = autoResetOffset;
    this.topicPartitions = topicPartitions;
  }

  @JsonGetter("bootstrapServers")
  public String getBootstrapServers() {
    return this.bootstrapServers;
  }

  @JsonGetter("groupId")
  public String getGroupId() {
    return this.groupId;
  }

  @JsonGetter("format")
  public String getFormat() {
    return this.format;
  }

  @JsonGetter("enableAutoCommit")
  public boolean getEnableAutoCommit() {
    return this.enableAutoCommit;
  }

  @JsonGetter("autoResetOffset")
  public String getAutoResetOffset() {
    return this.autoResetoffset;
  }

  @JsonGetter("topicPartitions")
  public List<TopicPartitionOffset> getTopicPartitions() {
    return this.topicPartitions;
  }

  public static class TopicPartitionOffset extends ISerializable {
    private String topic;

    private Integer partition;

    private Long offset;

    @JsonCreator
    public TopicPartitionOffset(
        @JsonProperty("topic") String topic,
        @JsonProperty("partition") Integer partition,
        @JsonProperty("offset") Long offset) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
    }

    @JsonGetter("topic")
    public String getTopic() {
      return this.topic;
    }

    @JsonGetter("partition")
    public Integer getPartition() {
      return this.partition;
    }

    @JsonGetter("offset")
    public Long getOffset() {
      return this.offset;
    }
  }
}
