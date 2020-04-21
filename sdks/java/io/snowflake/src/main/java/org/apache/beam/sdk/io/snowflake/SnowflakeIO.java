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

import static org.apache.beam.sdk.io.TextIO.readFiles;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.snowflake.credentials.KeyPairSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.OAuthTokenSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.UsernamePasswordSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on Snowflake.
 *
 * <p>SnowflakeIO uses <a href="https://docs.snowflake.net/manuals/user-guide/jdbc.html">Snowflake
 * JDBC</a> driver under the hood, but data isn't read/written using JDBC directly. Instead,
 * SnowflakeIO uses dedicated <b>COPY</b> operations to read/write data from/to Google Cloud
 * Storage.
 *
 * <p>To configure SnowflakeIO to read/write from your Snowflake instance, you have to provide a
 * {@link DataSourceConfiguration} using {@link
 * DataSourceConfiguration#create(SnowflakeCredentials)}, where {@link SnowflakeCredentials might be
 * created using {@link org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory}}.
 * Additionally one of {@link DataSourceConfiguration#withServerName(String)} or {@link
 * DataSourceConfiguration#withUrl(String)} must be used to tell SnowflakeIO which instance to use.
 * <br>
 * There are also other options available to configure connection to Snowflake:
 *
 * <ul>
 *   <li>{@link DataSourceConfiguration#withWarehouse(String)} to specify which Warehouse to use
 *   <li>{@link DataSourceConfiguration#withDatabase(String)} to specify which Database to connect
 *       to
 *   <li>{@link DataSourceConfiguration#withSchema(String)} to specify which schema to use
 *   <li>{@link DataSourceConfiguration#withRole(String)} to specify which role to use
 *   <li>{@link DataSourceConfiguration#withPortNumber(int)} to specify custom port of Snowflake
 *       instance
 * </ul>
 *
 * <p>For example:
 *
 * <pre>{@code
 * SnowflakeIO.DataSourceConfiguration dataSourceConfiguration =
 *     SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
 *         .withServerName(options.getServerName())
 *         .withWarehouse(options.getWarehouse())
 *         .withDatabase(options.getDatabase())
 *         .withSchema(options.getSchema);
 * }</pre>
 *
 * <h3>Reading from Snowflake</h3>
 *
 * <p>SnowflakeIO.Read returns a bounded collection of {@code T} as a {@code PCollection<T>}. T is
 * the type returned by the provided {@link CsvMapper}.
 *
 * <p>For example
 *
 * <pre>{@code
 * Location location = Location.of(storageIntegration, stagingBucketName);
 * PCollection<GenericRecord> items = pipeline.apply(
 *  SnowflakeIO.<GenericRecord>read()
 *    .withDataSourceConfiguration(dataSourceConfiguration)
 *    .fromQuery(QUERY)
 *    .via(location)
 *    .withCsvMapper(mapper)
 *    .withCoder(coder));
 * }</pre>
 *
 * <h3>Writing to Snowflake</h3>
 *
 * <p>SnowflakeIO.Write supports writing records into a database. It writes a {@link PCollection<T>}
 * to the database by converting each T into a {@link Object[]} via a user-provided {@link
 * UserDataMapper}.
 *
 * <p>For example
 *
 * <pre>{@code
 * Location location = Location.of(storageIntegration, stagingBucketName);
 * items.apply(
 *     SnowflakeIO.<KV<Integer, String>>write()
 *         .withDataSourceConfiguration(dataSourceConfiguration)
 *         .to(table)
 *         .via(location)
 *         .withUserDataMapper(maper);
 * }</pre>
 */
public class SnowflakeIO {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeIO.class);

  private static final String CSV_QUOTE_CHAR = "'";

  /**
   * Read data from Snowflake via COPY statement via user-defined {@link SnowflakeService}.
   *
   * @param snowflakeService user-defined {@link SnowflakeService}
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read(
      SnowflakeService snowflakeService, SnowflakeCloudProvider snowFlakeCloudProvider) {
    return new AutoValue_SnowflakeIO_Read.Builder<T>()
        .setSnowflakeService(snowflakeService)
        .setSnowflakeCloudProvider(snowFlakeCloudProvider)
        .build();
  }

  /**
   * Read data from Snowflake via COPY statement via default {@link SnowflakeServiceImpl}.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return read(new SnowflakeServiceImpl(), new GCSProvider());
  }

  /**
   * Interface for user-defined function mapping parts of CSV line into T. Used for
   * SnowflakeIO.Read.
   *
   * @param <T> Type of data to be read.
   */
  @FunctionalInterface
  public interface CsvMapper<T> extends Serializable {
    T mapRow(String[] parts) throws Exception;
  }

  /**
   * Interface for user-defined function mapping T into array of Objects. Used for
   * SnowflakeIO.Write.
   *
   * @param <T> Type of data to be written.
   */
  @FunctionalInterface
  public interface UserDataMapper<T> extends Serializable {
    Object[] mapRow(T element);
  }

  /**
   * Write data to Snowflake via COPY statement via user-defined {@link SnowflakeService}.
   *
   * @param <T> Type of data to be written.
   * @param snowflakeService user-defined {@link SnowflakeService}
   */
  public static <T> Write<T> write(
      SnowflakeService snowflakeService, SnowflakeCloudProvider cloudProvider) {
    return new AutoValue_SnowflakeIO_Write.Builder<T>()
        .setFileNameTemplate("output*")
        .setParallelization(true)
        .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .setWriteDisposition(WriteDisposition.APPEND)
        .setSnowflakeService(snowflakeService)
        .setSnowflakeCloudProvider(cloudProvider)
        .build();
  }

  /**
   * Write data to Snowflake via COPY statement via default {@link SnowflakeServiceImpl}.
   *
   * @param <T> Type of data to be written.
   */
  public static <T> Write<T> write() {
    return write(new SnowflakeServiceImpl(), new GCSProvider());
  }

  /** Implementation of {@link #read()}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract String getQuery();

    @Nullable
    abstract String getTable();

    @Nullable
    abstract Location getLocation();

    @Nullable
    abstract CsvMapper<T> getCsvMapper();

    @Nullable
    abstract Coder<T> getCoder();

    @Nullable
    abstract SnowflakeService getSnowflakeService();

    @Nullable
    abstract SnowflakeCloudProvider getSnowflakeCloudProvider();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setQuery(String query);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setLocation(Location location);

      abstract Builder<T> setCsvMapper(CsvMapper<T> csvMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setSnowflakeService(SnowflakeService snowflakeService);

      abstract Builder<T> setSnowflakeCloudProvider(SnowflakeCloudProvider snowFlakeCloudProvider);

      abstract Read<T> build();
    }

    public Read<T> withDataSourceConfiguration(final DataSourceConfiguration config) {

      try {
        Connection connection = config.buildDatasource().getConnection();
        connection.close();
      } catch (SQLException e) {
        throw new IllegalArgumentException(
            "Invalid DataSourceConfiguration. Underlying cause: " + e);
      }
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public Read<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public Read<T> fromQuery(String query) {
      return toBuilder().setQuery(query).build();
    }

    public Read<T> fromTable(String table) {
      return toBuilder().setTable(table).build();
    }

    public Read<T> via(Location location) {
      return toBuilder().setLocation(location).build();
    }

    public Read<T> withCsvMapper(CsvMapper<T> csvMapper) {
      return toBuilder().setCsvMapper(csvMapper).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      Location loc = getLocation();
      checkArguments();

      String gcpTmpDirName = makeTmpDirName();
      PCollection<T> output;
      PCollection<Void> emptyCollection = input.apply(Create.of((Void) null));

      output =
          emptyCollection
              .apply(
                  ParDo.of(
                      new CopyIntoStageFn(
                          getDataSourceProviderFn(),
                          getQuery(),
                          getTable(),
                          loc.getStorageIntegration(),
                          loc.getStagingBucketName(),
                          gcpTmpDirName,
                          getSnowflakeService(),
                          getSnowflakeCloudProvider())))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches())
              .apply(readFiles())
              .apply(ParDo.of(new MapCsvToStringArrayFn()))
              .apply(ParDo.of(new MapStringArrayToUserDataFn<>(getCsvMapper())));

      output.setCoder(getCoder());

      emptyCollection
          .apply(Wait.on(output))
          .apply(
              ParDo.of(
                  new CleanTmpFilesFromGcsFn(
                      loc.getStagingBucketName(), gcpTmpDirName, getSnowflakeCloudProvider())));

      return output;
    }

    private void checkArguments() {

      Location loc = getLocation();
      // Either table or query is required. If query is present, it's being used, table is used
      // otherwise
      checkArgument(loc != null, "via() is required");
      checkArgument(
          loc.getStorageIntegration() != null, "location with storageIntegration is required");
      checkArgument(
          loc.getStagingBucketName() != null, "location with stagingBucketName is required");

      checkArgument(
          getQuery() != null || getTable() != null, "fromTable() or fromQuery() is required");
      checkArgument(
          !(getQuery() != null && getTable() != null),
          "fromTable() and fromQuery() is not allowed together");
      checkArgument(getCsvMapper() != null, "withCsvMapper() is required");
      checkArgument(getCoder() != null, "withCoder() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");
    }

    private String makeTmpDirName() {
      return String.format(
          "sf_copy_csv_%s_%s",
          new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()),
          UUID.randomUUID().toString().subSequence(0, 8) // first 8 chars of UUID should be enough
          );
    }

    private static class CopyIntoStageFn extends DoFn<Object, String> {
      private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
      private final String source;
      private final String storageIntegration;
      private final String stagingBucketName;
      private final String tmpDirName;
      private final SnowflakeService snowflakeService;
      private final SnowflakeCloudProvider cloudProvider;

      private CopyIntoStageFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn,
          String query,
          String table,
          String storageIntegration,
          String stagingBucketName,
          String tmpDirName,
          SnowflakeService snowflakeService,
          SnowflakeCloudProvider cloudProvider) {
        this.dataSourceProviderFn = dataSourceProviderFn;
        this.storageIntegration = storageIntegration;
        this.stagingBucketName = stagingBucketName;
        this.tmpDirName = tmpDirName;
        this.snowflakeService = snowflakeService;
        this.cloudProvider = cloudProvider;

        if (query != null) {
          // Query must be surrounded with brackets
          this.source = String.format("(%s)", query);
        } else {
          this.source = table;
        }
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String stagingBucketDir = this.cloudProvider.formatCloudPath(stagingBucketName, tmpDirName);

        String output =
            snowflakeService.copyIntoStage(
                dataSourceProviderFn,
                source,
                storageIntegration,
                stagingBucketDir,
                this.cloudProvider);

        context.output(output);
      }
    }

    public static class MapCsvToStringArrayFn extends DoFn<String, String[]> {
      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        String csvLine = c.element();
        CSVParser parser = new CSVParserBuilder().withQuoteChar(CSV_QUOTE_CHAR.charAt(0)).build();
        String[] parts = parser.parseLine(csvLine);
        c.output(parts);
      }
    }

    private static class MapStringArrayToUserDataFn<T> extends DoFn<String[], T> {
      private final CsvMapper<T> csvMapper;

      public MapStringArrayToUserDataFn(CsvMapper<T> csvMapper) {
        this.csvMapper = csvMapper;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        context.output(csvMapper.mapRow(context.element()));
      }
    }

    public static class CleanTmpFilesFromGcsFn extends DoFn<Object, Object> {
      private final String stagingBucketName;
      private final String bucketPath;
      private final SnowflakeCloudProvider snowFlakeCloudProvider;

      public CleanTmpFilesFromGcsFn(
          String stagingBucketName,
          String bucketPath,
          SnowflakeCloudProvider snowFlakeCloudProvider) {
        this.stagingBucketName = stagingBucketName;
        this.bucketPath = bucketPath;
        this.snowFlakeCloudProvider = snowFlakeCloudProvider;
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        snowFlakeCloudProvider.removeFiles(stagingBucketName, bucketPath);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      Location loc = getLocation();

      super.populateDisplayData(builder);
      if (getQuery() != null) {
        builder.add(DisplayData.item("query", getQuery()));
      }
      if (getTable() != null) {
        builder.add(DisplayData.item("table", getTable()));
      }
      builder.add(DisplayData.item("storageIntegration", loc.getStorageIntegration()));
      builder.add(DisplayData.item("stagingBucketName", loc.getStagingBucketName()));
      builder.add(DisplayData.item("csvMapper", getCsvMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  /**
   * A POJO describing a {@link DataSource}, providing all properties allowing to create a {@link
   * DataSource}.
   */
  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {
    @Nullable
    public abstract ValueProvider<String> getUrl();

    @Nullable
    public abstract ValueProvider<String> getUsername();

    @Nullable
    public abstract ValueProvider<String> getPassword();

    @Nullable
    public abstract ValueProvider<PrivateKey> getPrivateKey();

    @Nullable
    public abstract ValueProvider<String> getOauthToken();

    @Nullable
    public abstract ValueProvider<String> getDatabase();

    @Nullable
    public abstract ValueProvider<String> getWarehouse();

    @Nullable
    public abstract ValueProvider<String> getSchema();

    @Nullable
    public abstract ValueProvider<String> getServerName();

    @Nullable
    public abstract ValueProvider<Integer> getPortNumber();

    @Nullable
    public abstract ValueProvider<String> getRole();

    @Nullable
    public abstract ValueProvider<String> getAuthenticator();

    @Nullable
    public abstract ValueProvider<Integer> getLoginTimeout();

    @Nullable
    public abstract ValueProvider<Boolean> getSsl();

    @Nullable
    public abstract DataSource getDataSource();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUrl(ValueProvider<String> url);

      abstract Builder setUsername(ValueProvider<String> username);

      abstract Builder setPassword(ValueProvider<String> password);

      abstract Builder setPrivateKey(ValueProvider<PrivateKey> privateKey);

      abstract Builder setOauthToken(ValueProvider<String> oauthToken);

      abstract Builder setDatabase(ValueProvider<String> database);

      abstract Builder setWarehouse(ValueProvider<String> warehouse);

      abstract Builder setSchema(ValueProvider<String> schema);

      abstract Builder setServerName(ValueProvider<String> serverName);

      abstract Builder setPortNumber(ValueProvider<Integer> portNumber);

      abstract Builder setRole(ValueProvider<String> role);

      abstract Builder setAuthenticator(ValueProvider<String> authenticator);

      abstract Builder setLoginTimeout(ValueProvider<Integer> loginTimeout);

      abstract Builder setSsl(ValueProvider<Boolean> ssl);

      abstract Builder setDataSource(DataSource dataSource);

      abstract DataSourceConfiguration build();
    }

    public static DataSourceConfiguration create(DataSource dataSource) {
      checkArgument(dataSource instanceof Serializable, "dataSource must be Serializable");
      return new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder()
          .setDataSource(dataSource)
          .build();
    }

    public static DataSourceConfiguration create(SnowflakeCredentials credentials) {
      return credentials.createSnowflakeDataSourceConfiguration();
    }

    public static DataSourceConfiguration create(UsernamePasswordSnowflakeCredentials credentials) {
      return new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder()
          .setUsername(ValueProvider.StaticValueProvider.of(credentials.getUsername()))
          .setPassword(ValueProvider.StaticValueProvider.of(credentials.getPassword()))
          .build();
    }

    public static DataSourceConfiguration create(KeyPairSnowflakeCredentials credentials) {
      return new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder()
          .setUsername(ValueProvider.StaticValueProvider.of(credentials.getUsername()))
          .setPrivateKey(ValueProvider.StaticValueProvider.of(credentials.getPrivateKey()))
          .build();
    }

    public static DataSourceConfiguration create(OAuthTokenSnowflakeCredentials credentials) {
      return new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder()
          .setOauthToken(ValueProvider.StaticValueProvider.of(credentials.getToken()))
          .build();
    }

    public DataSourceConfiguration withUrl(String url) {
      return withUrl(ValueProvider.StaticValueProvider.of(url));
    }

    public DataSourceConfiguration withUrl(ValueProvider<String> url) {
      checkArgument(
          url.get().startsWith("jdbc:snowflake://"),
          "url must have format: jdbc:snowflake://<account_name>.snowflakecomputing.com");
      checkArgument(
          url.get().endsWith("snowflakecomputing.com"),
          "url must have format: jdbc:snowflake://<account_name>.snowflakecomputing.com");
      return builder().setUrl(url).build();
    }

    public DataSourceConfiguration withDatabase(String database) {
      return withDatabase(ValueProvider.StaticValueProvider.of(database));
    }

    public DataSourceConfiguration withDatabase(ValueProvider<String> database) {
      return builder().setDatabase(database).build();
    }

    public DataSourceConfiguration withWarehouse(String warehouse) {
      return withWarehouse(ValueProvider.StaticValueProvider.of(warehouse));
    }

    public DataSourceConfiguration withWarehouse(ValueProvider<String> warehouse) {
      return builder().setWarehouse(warehouse).build();
    }

    public DataSourceConfiguration withSchema(String schema) {
      return withSchema(ValueProvider.StaticValueProvider.of(schema));
    }

    public DataSourceConfiguration withSchema(ValueProvider<String> schema) {
      return builder().setSchema(schema).build();
    }

    public DataSourceConfiguration withServerName(String withServerName) {
      return withServerName(ValueProvider.StaticValueProvider.of(withServerName));
    }

    public DataSourceConfiguration withServerName(ValueProvider<String> withServerName) {
      checkArgument(
          withServerName.get().endsWith("snowflakecomputing.com"),
          "serverName must be in format <account_name>.snowflakecomputing.com");
      return builder().setServerName(withServerName).build();
    }

    public DataSourceConfiguration withPortNumber(int portNumber) {
      return withPortNumber(ValueProvider.StaticValueProvider.of(portNumber));
    }

    public DataSourceConfiguration withPortNumber(ValueProvider<Integer> portNumber) {
      return builder().setPortNumber(portNumber).build();
    }

    public DataSourceConfiguration withRole(String role) {
      return withRole(ValueProvider.StaticValueProvider.of(role));
    }

    public DataSourceConfiguration withRole(ValueProvider<String> role) {
      return builder().setRole(role).build();
    }

    public DataSourceConfiguration withAuthenticator(String authenticator) {
      return withAuthenticator(ValueProvider.StaticValueProvider.of(authenticator));
    }

    public DataSourceConfiguration withAuthenticator(ValueProvider<String> authenticator) {
      return builder().setAuthenticator(authenticator).build();
    }

    public DataSourceConfiguration withLoginTimeout(Integer loginTimeout) {
      return withLoginTimeout(ValueProvider.StaticValueProvider.of(loginTimeout));
    }

    public DataSourceConfiguration withLoginTimeout(ValueProvider<Integer> loginTimeout) {
      return builder().setLoginTimeout(loginTimeout).build();
    }

    void populateDisplayData(DisplayData.Builder builder) {
      if (getDataSource() != null) {
        builder.addIfNotNull(DisplayData.item("dataSource", getDataSource().getClass().getName()));
      } else {
        builder.addIfNotNull(DisplayData.item("jdbcUrl", getUrl()));
        builder.addIfNotNull(DisplayData.item("username", getUsername()));
      }
    }

    public DataSource buildDatasource() {
      if (getDataSource() == null) {
        SnowflakeBasicDataSource basicDataSource = new SnowflakeBasicDataSource();

        if (getUrl() != null) {
          basicDataSource.setUrl(getUrl().get());
        }
        if (getUsername() != null) {
          basicDataSource.setUser(getUsername().get());
        }
        if (getPassword() != null) {
          basicDataSource.setPassword(getPassword().get());
        }
        if (getPrivateKey() != null) {
          basicDataSource.setPrivateKey(getPrivateKey().get());
        }
        if (getDatabase() != null) {
          basicDataSource.setDatabaseName(getDatabase().get());
        }
        if (getWarehouse() != null) {
          basicDataSource.setWarehouse(getWarehouse().get());
        }
        if (getSchema() != null) {
          basicDataSource.setSchema(getSchema().get());
        }
        if (getServerName() != null) {
          basicDataSource.setServerName(getServerName().get());
        }
        if (getPortNumber() != null) {
          basicDataSource.setPortNumber(getPortNumber().get());
        }
        if (getRole() != null) {
          basicDataSource.setRole(getRole().get());
        }
        if (getAuthenticator() != null) {
          basicDataSource.setAuthenticator(getAuthenticator().get());
        }
        if (getLoginTimeout() != null) {
          try {
            basicDataSource.setLoginTimeout(getLoginTimeout().get());
          } catch (SQLException e) {
            throw new RuntimeException("Failed to setLoginTimeout");
          }
        }
        if (getOauthToken() != null) {
          basicDataSource.setOauthToken(getOauthToken().get());
        }
        return basicDataSource;
      }
      return getDataSource();
    }
  }

  public static class DataSourceProviderFromDataSourceConfiguration
      implements SerializableFunction<Void, DataSource>, HasDisplayData {
    private static final ConcurrentHashMap<DataSourceConfiguration, DataSource> instances =
        new ConcurrentHashMap<>();
    private final DataSourceConfiguration config;

    private DataSourceProviderFromDataSourceConfiguration(DataSourceConfiguration config) {
      this.config = config;
    }

    public static SerializableFunction<Void, DataSource> of(DataSourceConfiguration config) {
      return new DataSourceProviderFromDataSourceConfiguration(config);
    }

    @Override
    public DataSource apply(Void input) {
      return instances.computeIfAbsent(config, (config) -> config.buildDatasource());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      config.populateDisplayData(builder);
    }
  }

  /** Implementation of {@link #write()}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract String getTable();

    @Nullable
    abstract String getQuery();

    @Nullable
    abstract Location getLocation();

    @Nullable
    abstract String getFileNameTemplate();

    @Nullable
    abstract Boolean getParallelization();

    @Nullable
    abstract WriteDisposition getWriteDisposition();

    @Nullable
    abstract CreateDisposition getCreateDisposition();

    @Nullable
    abstract UserDataMapper getUserDataMapper();

    @Nullable
    abstract SFTableSchema getTableSchema();

    @Nullable
    abstract SnowflakeService getSnowflakeService();

    @Nullable
    abstract SnowflakeCloudProvider getSnowflakeCloudProvider();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setQuery(String query);

      abstract Builder<T> setLocation(Location location);

      abstract Builder<T> setFileNameTemplate(String fileNameTemplate);

      abstract Builder<T> setParallelization(Boolean parallelization);

      abstract Builder<T> setUserDataMapper(UserDataMapper userDataMapper);

      abstract Builder<T> setWriteDisposition(WriteDisposition writeDisposition);

      abstract Builder<T> setCreateDisposition(CreateDisposition createDisposition);

      abstract Builder<T> setTableSchema(SFTableSchema tableSchema);

      abstract Builder<T> setSnowflakeService(SnowflakeService snowflakeService);

      abstract Builder<T> setSnowflakeCloudProvider(SnowflakeCloudProvider cloudProvider);

      abstract Write<T> build();
    }

    public Write<T> withDataSourceConfiguration(final DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public Write<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public Write<T> to(String table) {
      return toBuilder().setTable(table).build();
    }

    public Write<T> withQueryTransformation(String query) {
      return toBuilder().setQuery(query).build();
    }

    public Write<T> via(Location location) {
      return toBuilder().setLocation(location).build();
    }

    public Write<T> withFileNameTemplate(String fileNameTemplate) {
      return toBuilder().setFileNameTemplate(fileNameTemplate).build();
    }

    public Write<T> withParallelization(Boolean parallelization) {
      return toBuilder().setParallelization(parallelization).build();
    }

    public Write<T> withUserDataMapper(UserDataMapper userDataMapper) {
      return toBuilder().setUserDataMapper(userDataMapper).build();
    }

    public Write<T> withWriteDisposition(WriteDisposition writeDisposition) {
      return toBuilder().setWriteDisposition(writeDisposition).build();
    }

    public Write<T> withCreateDisposition(CreateDisposition createDisposition) {
      return toBuilder().setCreateDisposition(createDisposition).build();
    }

    public Write<T> withTableSchema(SFTableSchema tableSchema) {
      return toBuilder().setTableSchema(tableSchema).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      checkArguments();

      String path =
          this.getSnowflakeService().createCloudStoragePath(getLocation().getStagingBucketName());
      getLocation().setFilesPath(path);

      PCollection files = writeToFiles(input, path);

      files =
          (PCollection)
              files.apply("Create list of files to copy", Combine.globally(new Concatenate()));
      PCollection out = (PCollection) files.apply("Copy files to table", copyToTable());
      out.setCoder(StringUtf8Coder.of());

      return PDone.in(out.getPipeline());
    }

    private void checkArguments() {
      Location location = getLocation();

      checkArgument(location != null, "via() is required");

      checkArgument(getUserDataMapper() != null, "withUserDataMapper() is required");

      checkArgument(getTable() != null, "withTable() is required");

      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");
    }

    private PCollection writeToFiles(PCollection<T> input, String outputDirectory) {
      class Parse extends DoFn<KV<T, String>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(c.element().getValue());
        }
      }

      PCollection mappedUserData =
          input
              .apply(
                  "Map user data to Objects array",
                  ParDo.of(new MapUserDataObjectsArrayFn<T>(getUserDataMapper())))
              .apply("Map Objects array to CSV lines", ParDo.of(new MapObjectsArrayToCsvFn()))
              .setCoder(StringUtf8Coder.of());

      WriteFilesResult filesResult =
          (WriteFilesResult)
              mappedUserData.apply(
                  "Write files to specified location",
                  FileIO.write()
                      .via((FileIO.Sink) new CSVSink())
                      .to(outputDirectory)
                      .withSuffix(".csv")
                      .withCompression(Compression.GZIP));

      return (PCollection)
          filesResult
              .getPerDestinationOutputFilenames()
              .apply("Parse KV filenames to Strings", ParDo.of(new Parse()));
    }

    private ParDo.SingleOutput<Object, Object> copyToTable() {
      return ParDo.of(
          new CopyToTableFn<>(
              getDataSourceProviderFn(),
              getTable(),
              getQuery(),
              getLocation(),
              getCreateDisposition(),
              getWriteDisposition(),
              getTableSchema(),
              getSnowflakeService(),
              getSnowflakeCloudProvider()));
    }
  }

  private static class MapUserDataObjectsArrayFn<T> extends DoFn<T, Object[]> {
    private final UserDataMapper<T> csvMapper;

    public MapUserDataObjectsArrayFn(UserDataMapper<T> csvMapper) {
      this.csvMapper = csvMapper;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      context.output(csvMapper.mapRow(context.element()));
    }
  }

  /**
   * Custom DoFn that maps {@link Object[]} into CSV line to be saved to Snowflake.
   *
   * <p>Adds Snowflake-specific quotations around strings.
   */
  private static class MapObjectsArrayToCsvFn extends DoFn<Object[], String> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      List<Object> csvItems = new ArrayList<>();
      for (Object o : context.element()) {
        if (o instanceof String) {
          String field = (String) o;
          field = field.replace("'", "''");
          field = quoteField(field);

          csvItems.add(field);
        } else {
          csvItems.add(o);
        }
      }
      context.output(Joiner.on(",").useForNull("").join(csvItems));
    }

    private String quoteField(String field) {
      return quoteField(field, CSV_QUOTE_CHAR);
    }

    private String quoteField(String field, String quotation) {
      return String.format("%s%s%s", quotation, field, quotation);
    }
  }

  private static class CopyToTableFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final String source;
    private final String table;
    private final Location location;
    private final SFTableSchema tableSchema;
    private final WriteDisposition writeDisposition;
    private final CreateDisposition createDisposition;
    private final SnowflakeService snowflakeService;

    CopyToTableFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        String table,
        String query,
        Location location,
        CreateDisposition createDisposition,
        WriteDisposition writeDisposition,
        SFTableSchema tableSchema,
        SnowflakeService snowflakeService,
        SnowflakeCloudProvider cloudProvider) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.table = table;
      this.tableSchema = tableSchema;
      this.location = location;
      this.createDisposition = createDisposition;
      this.writeDisposition = writeDisposition;
      this.snowflakeService = snowflakeService;

      if (query != null) {
        this.source = String.format("(%s)", query);
      } else {
        String directory = location.getFilesLocationForCopy();
        this.source = cloudProvider.transformSnowflakePathToCloudPath(directory);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      LOG.error("ERROR" + (tableSchema == null));

      snowflakeService.copyToTable(
          dataSourceProviderFn,
          (List<String>) context.element(),
          table,
          tableSchema,
          source,
          createDisposition,
          writeDisposition,
          location);
    }
  }
}
