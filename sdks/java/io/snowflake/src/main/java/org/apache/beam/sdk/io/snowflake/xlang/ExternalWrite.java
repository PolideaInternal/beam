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
package org.apache.beam.sdk.io.snowflake.xlang;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.snowflake.Location;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.UsernamePasswordSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Exposes {@link SnowflakeIO.Write} as an external transform for cross-language usage. */
@Experimental
@AutoService(ExternalTransformRegistrar.class)
public final class ExternalWrite implements ExternalTransformRegistrar {
  public ExternalWrite() {}

  public static final String URN = "beam:external:java:snowflake:write:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
    return ImmutableMap.of(URN, WriteBuilder.class);
  }

  /** Parameters class to expose the transform to an external SDK. */
  public static class WriteConfiguration extends Configuration {
    private SnowflakeTableSchema tableSchema;
    private CreateDisposition createDisposition;
    private WriteDisposition writeDisposition;

    public void setTableSchema(String tableSchema) {
      ObjectMapper mapper = new ObjectMapper();

      try {
        this.tableSchema = mapper.readValue(tableSchema, SnowflakeTableSchema.class);
      } catch (IOException e) {
        throw new RuntimeException("Format of provided table schema is invalid");
      }
    }

    public void setCreateDisposition(String createDisposition) {
      this.createDisposition = CreateDisposition.valueOf(createDisposition);
    }

    public void setWriteDisposition(String writeDisposition) {
      this.writeDisposition = WriteDisposition.valueOf(writeDisposition);
    }

    public SnowflakeTableSchema getTableSchema() {
      return tableSchema;
    }

    public CreateDisposition getCreateDisposition() {
      return createDisposition;
    }

    public WriteDisposition getWriteDisposition() {
      return writeDisposition;
    }
  }

  public static class WriteBuilder
      implements ExternalTransformBuilder<WriteConfiguration, PCollection<byte[]>, PDone> {
    public WriteBuilder() {}

    @Override
    public PTransform<PCollection<byte[]>, PDone> buildExternal(WriteConfiguration c) {

      Location location = Location.of(c.getStorageIntegration(), c.getStagingBucketName());

      SerializableFunction<Void, DataSource> dataSourceSerializableFunction =
          SnowflakeIO.DataSourceProviderFromDataSourceConfiguration.of(
              SnowflakeIO.DataSourceConfiguration.create(
                      new UsernamePasswordSnowflakeCredentials(c.getUsername(), c.getPassword()))
                  .withServerName(c.getServerName())
                  .withDatabase(c.getDatabase())
                  .withSchema(c.getSchema()));

      return SnowflakeIO.<byte[]>write()
          .via(location)
          .withDataSourceProviderFn(dataSourceSerializableFunction)
          .withTableSchema(c.getTableSchema())
          .withCreateDisposition(c.getCreateDisposition())
          .withWriteDisposition(c.getWriteDisposition())
          .withUserDataMapper(getStringCsvMapper())
          .withQueryTransformation(c.getQuery())
          .to(c.getTable());
    }
  }

  public static SnowflakeIO.UserDataMapper<List<byte[]>> getStringCsvMapper() {
    return (SnowflakeIO.UserDataMapper<List<byte[]>>)
        recordLine -> recordLine.stream().map(String::new).toArray();
  }
}
