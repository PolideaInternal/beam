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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.snowflake.credentials.KeyPairSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.OAuthTokenSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.UsernamePasswordSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
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
  public static class Configuration {
    private String serverName;
    private String username;
    private String password;
    private String privateKeyFile;
    private String privateKeyPassword;
    private String oAuthToken;
    private String database;
    private String schema;
    private String table;
    private SFTableSchema tableSchema;
    private String query;
    private String stagingBucketName;
    private String storageIntegration;
    private CreateDisposition createDisposition;
    private WriteDisposition writeDisposition;
    private Boolean parallelization;

    public void setServerName(String serverName) {
      this.serverName = serverName;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public void setPrivateKeyFile(String privateKeyFile) {
      this.privateKeyFile = privateKeyFile;
    }

    public void setPrivateKeyPassword(String privateKeyPassword) {
      this.privateKeyPassword = privateKeyPassword;
    }

    public void setOAuthToken(String oAuthToken) {
      this.oAuthToken = oAuthToken;
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

    public void setTableSchema(String tableSchema) {
      ObjectMapper mapper = new ObjectMapper();

      try {
        this.tableSchema = mapper.readValue(tableSchema, SFTableSchema.class);
      } catch (IOException e) {
        throw new RuntimeException("Format of provided table schema is invalid");
      }
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

    public void setCreateDisposition(String createDisposition) {
      this.createDisposition = CreateDisposition.valueOf(createDisposition);
    }

    public void setWriteDisposition(String writeDisposition) {
      this.writeDisposition = WriteDisposition.valueOf(writeDisposition);
    }

    public void setParallelization(Boolean parallelization) {
      this.parallelization = parallelization;
    }
  }

  public static class WriteBuilder
      implements ExternalTransformBuilder<Configuration, PCollection<byte[]>, PDone> {
    public WriteBuilder() {}

    @Override
    public PTransform<PCollection<byte[]>, PDone> buildExternal(Configuration config) {

      SnowflakeIO.Write.Builder<byte[]> writeBuilder = new AutoValue_SnowflakeIO_Write.Builder<>();

      writeBuilder.setSnowflakeService(new SnowflakeServiceImpl());
      writeBuilder.setSnowflakeCloudProvider(new GCSProvider());

      SnowflakeCredentials credentials = createCredentials(config);
      writeBuilder.setLocation(Location.of(config.storageIntegration, config.stagingBucketName));
      writeBuilder.setDataSourceProviderFn(
          SnowflakeIO.DataSourceProviderFromDataSourceConfiguration.of(
              SnowflakeIO.DataSourceConfiguration.create(credentials)
                  .withServerName(config.serverName)
                  .withDatabase(config.database)
                  .withSchema(config.schema)));
      if (config.table != null) {
        writeBuilder.setTable(config.table);
      }
      if (config.query != null) {
        writeBuilder.setQuery(config.query);
      }

      writeBuilder.setTableSchema(config.tableSchema);
      writeBuilder.setCreateDisposition(config.createDisposition);
      writeBuilder.setWriteDisposition(config.writeDisposition);
      writeBuilder.setParallelization(config.parallelization);

      // TODO hard-coded function. Planned to be implement in SNOW-158
      writeBuilder.setUserDataMapper(getLStringCsvMapper());

      return writeBuilder.build();
    }
  }

  public static SnowflakeIO.UserDataMapper<List<byte[]>> getLStringCsvMapper() {
    return (SnowflakeIO.UserDataMapper<List<byte[]>>)
        recordLine -> recordLine.stream().map(String::new).toArray();
  }

  private static SnowflakeCredentials createCredentials(ExternalWrite.Configuration config) {
    if (config.oAuthToken != null) {
      return new OAuthTokenSnowflakeCredentials(config.oAuthToken);
    } else if (config.privateKeyFile != null
        && config.privateKeyPassword != null
        && config.username != null) {
      return new KeyPairSnowflakeCredentials(
          config.username, config.privateKeyFile, config.privateKeyPassword);
    } else if (config.username != null && config.password != null) {
      return new UsernamePasswordSnowflakeCredentials(config.username, config.password);
    } else {
      throw new RuntimeException("No credentials given");
    }
  }
}
