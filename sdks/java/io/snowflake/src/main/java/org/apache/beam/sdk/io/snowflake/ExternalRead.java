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
package org.apache.beam.sdk.io.snowflake;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.snowflake.credentials.UsernamePasswordSnowflakeCredentials;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Exposes {@link SnowflakeIO.Read} as an external transform for cross-language usage. */
@Experimental
@AutoService(ExternalTransformRegistrar.class)
public final class ExternalRead implements ExternalTransformRegistrar {
  public ExternalRead() {}

  public static final String URN = "beam:external:java:snowflake:read:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
    return ImmutableMap.of(URN, ReadBuilder.class);
  }

  /** Parameters class to expose the transform to an external SDK. */
  public static class Configuration {
    private String serverName;
    private String username;
    private String password;
    private String database;
    private String schema;
    private String table;
    private String query;
    private String stagingBucketName;
    private String storageIntegration;

    public void setServerName(String serverName) {
      this.serverName = serverName;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public void setDatabase(String database) {
      this.database = database;
    }

    public void setSchema(String schema) {
      this.schema = schema;
    }

    public void setTable(String table) {
      this.table = table;
    }

    public void setQuery(String query) {
      this.query = query;
    }

    public void setStagingBucketName(String stagingBucketName) {
      this.stagingBucketName = stagingBucketName;
    }

    public void setStorageIntegration(String storageIntegration) {
      this.storageIntegration = storageIntegration;
    }
  }

  public static class ReadBuilder
      implements ExternalTransformBuilder<Configuration, PBegin, PCollection<byte[]>> {
    public ReadBuilder() {}

    @Override
    public PTransform<PBegin, PCollection<byte[]>> buildExternal(Configuration config) {

      SnowflakeIO.Read.Builder<byte[]> readBuilder = new AutoValue_SnowflakeIO_Read.Builder<>();

      readBuilder.setSnowflakeService(new SnowflakeServiceImpl());
      readBuilder.setSnowflakeCloudProvider(new GCSProvider());
      readBuilder.setLocation(Location.of(config.storageIntegration, config.stagingBucketName));
      readBuilder.setDataSourceProviderFn(
          SnowflakeIO.DataSourceProviderFromDataSourceConfiguration.of(
              SnowflakeIO.DataSourceConfiguration.create(
                      new UsernamePasswordSnowflakeCredentials(config.username, config.password))
                  .withServerName(config.serverName)
                  .withDatabase(config.database)
                  .withSchema(config.schema)));
      if (config.table != null) {
        readBuilder.setTable(config.table);
      }
      if (config.query != null) {
        readBuilder.setQuery(config.query);
      }

      readBuilder.setCsvMapper(CsvMapper.getCsvMapper());

      readBuilder.setCoder(ByteArrayCoder.of());

      return readBuilder.build();
    }
  }

  public static class CsvMapper implements Serializable {

    public static SnowflakeIO.CsvMapper getCsvMapper() {
      return (SnowflakeIO.CsvMapper<byte[]>)
          parts -> {
            String partsCSV = String.join(",", parts);

            return partsCSV.getBytes(Charset.defaultCharset());
          };
    }
  }
}
