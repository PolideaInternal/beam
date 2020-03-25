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

import com.google.api.gax.paging.Page;
import com.google.auto.value.AutoValue;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.ResultSet;
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
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
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
 * PCollection<GenericRecord> items = pipeline.apply(
 *  SnowflakeIO.<GenericRecord>read()
 *    .withDataSourceConfiguration(dataSourceConfiguration)
 *    .fromQuery(QUERY)
 *    .withStagingBucketName(stagingBucketName)
 *    .withIntegrationName(integrationName)
 *    .withCsvMapper(...)
 *    .withCoder(...));
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
 * items.apply(
 *     SnowflakeIO.<KV<Integer, String>>write()
 *         .withDataSourceConfiguration(dataSourceConfiguration)
 *         .to(table)
 *         .via(LocationFactory.of(options))
 *         .withUserDataMapper(...);
 * }</pre>
 */
public class SnowflakeIO {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeIO.class);

  private static final String CSV_QUOTE_CHAR = "'";
  private static final String CSV_QUOTE_CHAR_FOR_COPY = "''";

  /**
   * Read data from Snowflake via COPY statement via user-defined {@link SnowflakeService}.
   *
   * @param snowflakeService user-defined {@link SnowflakeService}
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read(SnowflakeService snowflakeService) {
    return new AutoValue_SnowflakeIO_Read.Builder<T>()
        .setSnowflakeService(snowflakeService)
        .build();
  }

  /**
   * Read data from Snowflake via COPY statement via default {@link SnowflakeServiceImpl}
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return read(new SnowflakeServiceImpl());
  }

  public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll(
      SnowflakeService snowflakeService) {
    return new AutoValue_SnowflakeIO_ReadAll.Builder<ParameterT, OutputT>()
        .setSnowflakeService(snowflakeService)
        .build();
  }

  public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
    return readAll(new SnowflakeServiceImpl());
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
   * Interface for user-defined function mapping T into array of Objects. Used for SnowflakeIO.Write
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
  public static <T> Write<T> write(SnowflakeService snowflakeService) {
    return new AutoValue_SnowflakeIO_Write.Builder<T>()
        .setFileNameTemplate(ValueProvider.StaticValueProvider.of("output*"))
        .setParallelization(ValueProvider.StaticValueProvider.of(true))
        .setCreateDisposition(
            ValueProvider.StaticValueProvider.of(CreateDisposition.CREATE_IF_NEEDED))
        .setWriteDisposition(ValueProvider.StaticValueProvider.of(WriteDisposition.APPEND))
        .setSnowflakeService(snowflakeService)
        .build();
  }

  /**
   * Write data to Snowflake via COPY statement via default {@link SnowflakeServiceImpl}
   *
   * @param <T> Type of data to be written.
   */
  public static <T> Write<T> write() {
    return write(new SnowflakeServiceImpl());
  }

  /** Implementation of {@link #read()} */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract String getQuery();

    @Nullable
    abstract String getTable();

    @Nullable
    abstract String getIntegrationName();

    @Nullable
    abstract String getStagingBucketName();

    @Nullable
    abstract CsvMapper<T> getCsvMapper();

    @Nullable
    abstract Coder<T> getCoder();

    @Nullable
    abstract SnowflakeService getSnowflakeService();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setQuery(String query);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setIntegrationName(String integrationName);

      abstract Builder<T> setStagingBucketName(String stagingBucketName);

      abstract Builder<T> setCsvMapper(CsvMapper<T> csvMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setSnowflakeService(SnowflakeService snowflakeService);

      abstract Read<T> build();
    }

    public Read<T> withDataSourceConfiguration(final DataSourceConfiguration config) {
      try {
        config.buildDatasource().getConnection();
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

    public Read<T> withStagingBucketName(String stagingBucketName) {
      return toBuilder().setStagingBucketName(stagingBucketName).build();
    }

    public Read<T> withIntegrationName(String integrationName) {
      return toBuilder().setIntegrationName(integrationName).build();
    }

    public Read<T> withCsvMapper(CsvMapper<T> csvMapper) {
      return toBuilder().setCsvMapper(csvMapper).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      // Either table or query is required. If query is present, it's being used, table is used
      // otherwise
      checkArgument(
          getQuery() != null || getTable() != null, "fromTable() or fromQuery() is required");
      checkArgument(getCsvMapper() != null, "withCsvMapper() is required");
      checkArgument(getCoder() != null, "withCoder() is required");
      checkArgument(getIntegrationName() != null, "withIntegrationName() is required");
      checkArgument(getStagingBucketName() != null, "withStagingBucketName() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      return input
          .apply(Create.of((Void) null))
          .apply(
              SnowflakeIO.<Void, T>readAll(getSnowflakeService())
                  .withDataSourceProviderFn(getDataSourceProviderFn())
                  .fromQuery(getQuery())
                  .fromTable(getTable())
                  .withCsvMapper(getCsvMapper())
                  .withStagingBucketName(getStagingBucketName())
                  .withIntegrationName(getIntegrationName())
                  .withCoder(getCoder()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (getQuery() != null) {
        builder.add(DisplayData.item("query", getQuery()));
      }
      if (getTable() != null) {
        builder.add(DisplayData.item("table", getTable()));
      }
      builder.add(DisplayData.item("integrationName", getIntegrationName()));
      builder.add(DisplayData.item("stagingBucketName", getStagingBucketName()));
      builder.add(DisplayData.item("csvMapper", getCsvMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  /** Implementation of {@link #readAll()} */
  @AutoValue
  public abstract static class ReadAll<ParameterT, OutputT>
      extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {

    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract String getQuery();

    @Nullable
    abstract String getTable();

    @Nullable
    abstract String getIntegrationName();

    @Nullable
    abstract String getStagingBucketName();

    @Nullable
    abstract CsvMapper<OutputT> getCsvMapper();

    @Nullable
    abstract Coder<OutputT> getCoder();

    @Nullable
    abstract SnowflakeService getSnowflakeService();

    abstract Builder<ParameterT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<ParameterT, OutputT> {
      abstract Builder<ParameterT, OutputT> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<ParameterT, OutputT> setQuery(String query);

      abstract Builder<ParameterT, OutputT> setTable(String table);

      abstract Builder<ParameterT, OutputT> setIntegrationName(String integrationName);

      abstract Builder<ParameterT, OutputT> setStagingBucketName(String stagingBucketName);

      abstract Builder<ParameterT, OutputT> setCsvMapper(CsvMapper<OutputT> csvMapper);

      abstract Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

      abstract Builder<ParameterT, OutputT> setSnowflakeService(SnowflakeService snowflakeService);

      abstract ReadAll<ParameterT, OutputT> build();
    }

    public ReadAll<ParameterT, OutputT> withDataSourceConfiguration(
        DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public ReadAll<ParameterT, OutputT> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public ReadAll<ParameterT, OutputT> fromQuery(String query) {
      return toBuilder().setQuery(query).build();
    }

    public ReadAll<ParameterT, OutputT> fromTable(String table) {
      return toBuilder().setTable(table).build();
    }

    public ReadAll<ParameterT, OutputT> withIntegrationName(String integrationName) {
      checkArgument(
          integrationName != null,
          "SnowflakeIO.readAll().withIntegrationName(integrationName) called with null integrationName");
      return toBuilder().setIntegrationName(integrationName).build();
    }

    public ReadAll<ParameterT, OutputT> withStagingBucketName(String stagingBucketName) {
      checkArgument(
          stagingBucketName != null,
          "SnowflakeIO.readAll().withStagingBucketName(stagingBucketName) called with null stagingBucketName");
      return toBuilder().setStagingBucketName(stagingBucketName).build();
    }

    public ReadAll<ParameterT, OutputT> withCsvMapper(CsvMapper<OutputT> csvMapper) {
      checkArgument(
          csvMapper != null,
          "SnowflakeIO.readAll().withCsvMapper(csvMapper) called with null csvMapper");
      return toBuilder().setCsvMapper(csvMapper).build();
    }

    public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
      checkArgument(coder != null, "SnowflakeIO.readAll().withCoder(coder) called with null coder");
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<ParameterT> input) {
      PCollection<OutputT> output;
      String gcpTmpDirName = makeTmpDirName();

      output =
          input
              .apply(
                  ParDo.of(
                      new CopyToExternalLocationFn<>(
                          getDataSourceProviderFn(),
                          getQuery(),
                          getTable(),
                          getIntegrationName(),
                          getStagingBucketName(),
                          gcpTmpDirName,
                          getSnowflakeService())))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches())
              .apply(readFiles())
              .apply(ParDo.of(new MapCsvToStringArrayFn()))
              .apply(ParDo.of(new MapStringArrayToUserDataFn<>(getCsvMapper())));

      output.setCoder(getCoder());

      input
          .apply(Wait.on(output))
          .apply(ParDo.of(new CleanTmpFilesFromGcsFn(getStagingBucketName(), gcpTmpDirName)));

      return output;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (getQuery() != null) {
        builder.add(DisplayData.item("query", getQuery()));
      }
      if (getTable() != null) {
        builder.add(DisplayData.item("table", getTable()));
      }
      builder.add(DisplayData.item("integrationName", getIntegrationName()));
      builder.add(DisplayData.item("stagingBucketName", getStagingBucketName()));
      builder.add(DisplayData.item("csvMapper", getCsvMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }

    private String makeTmpDirName() {
      return String.format(
          "sf_copy_csv_%s_%s",
          new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()),
          UUID.randomUUID().toString().subSequence(0, 8) // first 8 chars of UUID should be enough
          );
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

  public static class CleanTmpFilesFromGcsFn extends DoFn<Object, Object> {
    private final String bucketName;
    private final String bucketPath;

    public CleanTmpFilesFromGcsFn(String bucketName, String bucketPath) {
      this.bucketName = bucketName;
      this.bucketPath = bucketPath;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Storage storage = StorageOptions.getDefaultInstance().getService();
      Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(bucketPath));
      for (Blob blob : blobs.iterateAll()) {
        storage.delete(blob.getBlobId());
      }
    }
  }

  private static class MapStringArrayToUserDataFn<InputT, OutputT> extends DoFn<String[], OutputT> {
    private final CsvMapper<OutputT> csvMapper;

    public MapStringArrayToUserDataFn(CsvMapper<OutputT> csvMapper) {
      this.csvMapper = csvMapper;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      context.output(csvMapper.mapRow(context.element()));
    }
  }

  private static class CopyToExternalLocationFn<ParameterT> extends DoFn<ParameterT, String> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final String query;
    private final String table;
    private final String integrationName;
    private final String stagingBucketName;
    private final String tmpDirName;
    private final SnowflakeService snowflakeService;

    private DataSource dataSource;
    private Connection connection;

    private CopyToExternalLocationFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        String query,
        String table,
        String integrationName,
        String stagingBucketName,
        String tmpDirName,
        SnowflakeService snowflakeService) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.query = query;
      this.table = table;
      this.integrationName = integrationName;
      this.stagingBucketName = stagingBucketName;
      this.tmpDirName = tmpDirName;
      this.snowflakeService = snowflakeService;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);
      connection = dataSource.getConnection();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      String output =
          snowflakeService.executeCopyIntoLocation(
              connection, query, table, integrationName, stagingBucketName, tmpDirName);

      context.output(output);
    }

    @Teardown
    public void teardown() throws Exception {
      connection.close();
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

  /** Implementation of {@link #write()} */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract ValueProvider<String> getTable();

    @Nullable
    abstract ValueProvider<String> getQuery();

    @Nullable
    abstract ValueProvider<Location> getLocation();

    @Nullable
    abstract ValueProvider<String> getFileNameTemplate();

    @Nullable
    abstract ValueProvider<Boolean> getParallelization();

    @Nullable
    abstract ValueProvider<WriteDisposition> getWriteDisposition();

    @Nullable
    abstract ValueProvider<CreateDisposition> getCreateDisposition();

    @Nullable
    abstract UserDataMapper getUserDataMapper();

    @Nullable
    abstract SFTableSchema getTableSchema();

    @Nullable
    abstract SnowflakeService getSnowflakeService();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setTable(ValueProvider<String> table);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setLocation(ValueProvider<Location> location);

      abstract Builder<T> setFileNameTemplate(ValueProvider<String> fileNameTemplate);

      abstract Builder<T> setParallelization(ValueProvider<Boolean> parallelization);

      abstract Builder<T> setUserDataMapper(UserDataMapper userDataMapper);

      abstract Builder<T> setWriteDisposition(ValueProvider<WriteDisposition> writeDisposition);

      abstract Builder<T> setCreateDisposition(ValueProvider<CreateDisposition> createDisposition);

      abstract Builder<T> setTableSchema(SFTableSchema tableSchema);

      abstract Builder<T> setSnowflakeService(SnowflakeService snowflakeService);

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
      return to(ValueProvider.StaticValueProvider.of(table));
    }

    public Write<T> to(ValueProvider<String> table) {
      return toBuilder().setTable(table).build();
    }

    public Write<T> withQueryTransformation(String query) {
      return withQueryTransformation(ValueProvider.StaticValueProvider.of(query));
    }

    public Write<T> withQueryTransformation(ValueProvider<String> query) {
      return toBuilder().setQuery(query).build();
    }

    public Write<T> via(Location location) {
      return via(ValueProvider.StaticValueProvider.of(location));
    }

    public Write<T> via(ValueProvider<Location> location) {
      return toBuilder().setLocation(location).build();
    }

    public Write<T> withFileNameTemplate(ValueProvider<String> fileNameTemplate) {
      return toBuilder().setFileNameTemplate(fileNameTemplate).build();
    }

    public Write<T> withFileNameTemplate(String fileNameTemplate) {
      return withFileNameTemplate(ValueProvider.StaticValueProvider.of(fileNameTemplate));
    }

    public Write<T> withParallelization(ValueProvider<Boolean> parallelization) {
      return toBuilder().setParallelization(parallelization).build();
    }

    public Write<T> withParallelization(Boolean parallelization) {
      return withParallelization(ValueProvider.StaticValueProvider.of(parallelization));
    }

    public Write<T> withUserDataMapper(UserDataMapper userDataMapper) {
      return toBuilder().setUserDataMapper(userDataMapper).build();
    }

    public Write<T> withWriteDisposition(ValueProvider<WriteDisposition> writeDisposition) {
      return toBuilder().setWriteDisposition(writeDisposition).build();
    }

    public Write<T> withWriteDisposition(WriteDisposition writeDisposition) {
      return withWriteDisposition(ValueProvider.StaticValueProvider.of(writeDisposition));
    }

    public Write<T> withCreateDisposition(ValueProvider<CreateDisposition> createDisposition) {
      return toBuilder().setCreateDisposition(createDisposition).build();
    }

    public Write<T> withCreateDisposition(CreateDisposition createDisposition) {
      return withCreateDisposition(ValueProvider.StaticValueProvider.of(createDisposition));
    }

    public Write<T> withTableSchema(SFTableSchema tableSchema) {
      return toBuilder().setTableSchema(tableSchema).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      checkArgument((getLocation() != null), "withLocation() is required");

      checkArgument((getUserDataMapper() != null), "withUserDataMapper() is required");

      checkArgument(getTable() != null, "withTable() is required");

      if (getQuery() != null) {
        checkArgument(
            (getLocation().get().getStage() != null), "withQuery() requires stage as location");
      }

      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      checkArgument(getLocation() != null, "withLocation() is required");

      Location location = getLocation().get();

      PCollection files = writeToFiles(input, location.getFilesPath());

      if (location.isInternal()) {
        files = putFilesToInternalStage(files, location);
      }

      files =
          (PCollection)
              files.apply("Create list of files to copy", Combine.globally(new Concatenate()));
      PCollection out = (PCollection) files.apply("Copy files to table", copyToTable(location));
      out.setCoder(StringUtf8Coder.of());

      return PDone.in(out.getPipeline());
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
              .apply("Map Objects array to CSV lines", ParDo.of(new MapObjecsArrayToCsvFn()))
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

    private PCollection putFilesToInternalStage(PCollection pcol, Location location) {
      if (!getParallelization().get()) {
        pcol =
            (PCollection)
                pcol.apply("Combine all files into one flow", Combine.globally(new Concatenate()));
      }

      pcol = (PCollection) pcol.apply("Put files on stage", putToInternalStage(location));

      pcol.setCoder(StringUtf8Coder.of());
      return pcol;
    }

    private ParDo.SingleOutput<Object, Object> putToInternalStage(Location location) {
      return ParDo.of(
          new PutFn<>(
              getDataSourceProviderFn(),
              location.getFilesLocationForCopy(),
              location.getFilesPath(),
              getFileNameTemplate(),
              getParallelization(),
              getSnowflakeService()));
    }

    private ParDo.SingleOutput<Object, Object> copyToTable(Location location) {
      return ParDo.of(
          new CopyToTableFn<>(
              getDataSourceProviderFn(),
              getTable(),
              getQuery(),
              location,
              getCreateDisposition(),
              getWriteDisposition(),
              getTableSchema(),
              getSnowflakeService()));
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
  private static class MapObjecsArrayToCsvFn extends DoFn<Object[], String> {

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
    private final String filesPath;
    private final Location location;
    private final String table;
    private final SFTableSchema tableSchema;
    private final ValueProvider<WriteDisposition> writeDisposition;
    private final ValueProvider<CreateDisposition> createDisposition;
    private final SnowflakeService snowflakeService;

    private DataSource dataSource;
    private Connection connection;

    CopyToTableFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> table,
        ValueProvider<String> query,
        Location location,
        ValueProvider<CreateDisposition> createDisposition,
        ValueProvider<WriteDisposition> writeDisposition,
        SFTableSchema tableSchema,
        SnowflakeService snowflakeService) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.table = table.get();
      this.tableSchema = tableSchema;
      this.location = location;
      if (query != null) {
        String formattedQuery = String.format(query.get(), location.getFilesLocationForCopy());
        this.source = String.format("(%s)", formattedQuery);
      } else {
        String directory = location.getFilesLocationForCopy();
        this.source = directory.replace("gs://", "gcs://");
      }

      this.filesPath = location.getFilesPath();
      this.createDisposition = createDisposition;
      this.writeDisposition = writeDisposition;
      this.snowflakeService = snowflakeService;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);
      connection = dataSource.getConnection();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      LOG.error("ERROR" + (tableSchema == null));

      snowflakeService.executeCopyToTable(
          connection,
          (List<String>) context.element(),
          dataSource,
          table,
          tableSchema,
          source,
          location,
          createDisposition.get(),
          writeDisposition.get(),
          filesPath);
    }

    @Teardown
    public void teardown() throws Exception {
      if (connection != null) {
        connection.close();
      }
    }
  }

  private static class PutFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final String stage;
    private final String directory;
    private final ValueProvider<String> fileNameTemplate;
    private final ValueProvider<Boolean> parallelization;
    private final SnowflakeService snowflakeService;

    private DataSource dataSource;
    private Connection connection;

    PutFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        String stage,
        String file,
        ValueProvider<String> fileNameTemplate,
        ValueProvider<Boolean> parallelization,
        SnowflakeService snowflakeService) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.stage = stage;
      this.directory = file;
      this.fileNameTemplate = fileNameTemplate;
      this.parallelization = parallelization;
      this.snowflakeService = snowflakeService;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);
      connection = dataSource.getConnection();
    }

    /* Right now it is paralleled per each created file */
    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      snowflakeService.executePut(
          connection,
          context.element().toString(),
          stage,
          directory,
          fileNameTemplate.get(),
          parallelization.get(),
          resultSet -> {
            assert resultSet != null;
            getFilenamesFromPutOperation((ResultSet) resultSet, context);
          });
    }

    void getFilenamesFromPutOperation(ResultSet resultSet, ProcessContext context) {
      int indexOfNameOfFile = 2;
      try {
        while (resultSet.next()) {
          context.output((OutputT) resultSet.getString(indexOfNameOfFile));
        }
      } catch (SQLException e) {
        throw new RuntimeException("Unable run pipeline with PUT operation.", e);
      }
    }

    @Teardown
    public void teardown() throws Exception {
      connection.close();
    }
  }
}
