/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.snowflake.services;

import java.util.List;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.snowflake.Location;
import org.apache.beam.sdk.io.snowflake.SnowflakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class SnowflakeBatchServiceConfig extends ServiceConfig {
  private SerializableFunction<Void, DataSource> dataSourceProviderFn;
  private String source;

  private String table;
  private String integrationName;
  private List<String> filesList;
  private SFTableSchema tableSchema;

  private SnowflakeCloudProvider cloudProvider;
  private CreateDisposition createDisposition;
  private WriteDisposition writeDisposition;
  private Location location;
  private String stagingBucketDir;

  public SnowflakeBatchServiceConfig(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      String source,
      String storageIntegration,
      String stagingBucketDir,
      SnowflakeCloudProvider cloudProvider) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.source = source;
    this.integrationName = storageIntegration;
    this.stagingBucketDir = stagingBucketDir;
    this.cloudProvider = cloudProvider;
  }

  public SnowflakeBatchServiceConfig(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      List<String> filesList,
      String table,
      SFTableSchema tableSchema,
      String source,
      CreateDisposition createDisposition,
      WriteDisposition writeDisposition,
      Location location) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.filesList = filesList;
    this.tableSchema = tableSchema;
    this.source = source;
    this.table = table;
    this.createDisposition = createDisposition;
    this.writeDisposition = writeDisposition;
    this.location = location;
  }

  public SerializableFunction<Void, DataSource> getDataSourceProviderFn() {
    return dataSourceProviderFn;
  }

  public SnowflakeCloudProvider getCloudProvider() {
    return cloudProvider;
  }

  public String getSource() {
    return source;
  }

  public String getTable() {
    return table;
  }

  public String getIntegrationName() {
    return integrationName;
  }

  public String getStagingBucketDir() {
    return stagingBucketDir;
  }

  public List<String> getFilesList() {
    return filesList;
  }

  public SFTableSchema getTableSchema() {
    return tableSchema;
  }

  public CreateDisposition getCreateDisposition() {
    return createDisposition;
  }

  public WriteDisposition getWriteDisposition() {
    return writeDisposition;
  }

  public Location getLocation() {
    return location;
  }
}
