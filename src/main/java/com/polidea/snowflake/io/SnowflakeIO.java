package com.polidea.snowflake.io;

import static org.apache.beam.sdk.io.TextIO.readFiles;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.polidea.snowflake.io.credentials.KeyPairSnowflakeCredentials;
import com.polidea.snowflake.io.credentials.OAuthTokenSnowflakeCredentials;
import com.polidea.snowflake.io.credentials.SnowflakeCredentials;
import com.polidea.snowflake.io.credentials.UsernamePasswordSnowflakeCredentials;
import java.io.Serializable;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeIO {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeIO.class);

  /** Read data from a Snowflake using COPY method. */
  public static <T> Read<T> read() {
    return new AutoValue_SnowflakeIO_Read.Builder<T>().build();
  }

  public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
    return new AutoValue_SnowflakeIO_ReadAll.Builder<ParameterT, OutputT>().build();
  }

  @FunctionalInterface
  public interface CsvMapper<T> extends Serializable {
    T mapRow(String csvLine) throws Exception;
  }

  public static <T> Write<T> write() {
    return new AutoValue_SnowflakeIO_Write.Builder<T>()
        .setFileNameTemplate(ValueProvider.StaticValueProvider.of("output*"))
        .setParallelization(ValueProvider.StaticValueProvider.of(true))
        .build();
  }

  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract ValueProvider<String> getQuery();

    @Nullable
    abstract ValueProvider<String> getTable();

    @Nullable
    abstract ValueProvider<String> getIntegrationName();

    @Nullable
    abstract ValueProvider<String> getExternalLocation();

    @Nullable
    abstract CsvMapper<T> getCsvMapper();

    @Nullable
    abstract Coder<T> getCoder();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setTable(ValueProvider<String> table);

      abstract Builder<T> setIntegrationName(ValueProvider<String> integrationName);

      abstract Builder<T> setExternalLocation(ValueProvider<String> externalLocation);

      abstract Builder<T> setCsvMapper(CsvMapper<T> csvMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Read<T> build();
    }

    public Read<T> withDataSourceConfiguration(final DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public Read<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public Read<T> fromQuery(String query) {
      return fromQuery(ValueProvider.StaticValueProvider.of(query));
    }

    public Read<T> fromQuery(ValueProvider<String> query) {
      return toBuilder().setQuery(query).build();
    }

    public Read<T> fromTable(String table) {
      return fromTable(ValueProvider.StaticValueProvider.of(table));
    }

    public Read<T> fromTable(ValueProvider<String> table) {
      return toBuilder().setTable(table).build();
    }

    public Read<T> withExternalLocation(String externalLocation) {
      return withExternalLocation(ValueProvider.StaticValueProvider.of(externalLocation));
    }

    public Read<T> withExternalLocation(ValueProvider<String> externalLocation) {
      return toBuilder().setExternalLocation(externalLocation).build();
    }

    public Read<T> withIntegrationName(String integrationName) {
      return withIntegrationName(ValueProvider.StaticValueProvider.of(integrationName));
    }

    public Read<T> withIntegrationName(ValueProvider<String> integrationName) {
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
      checkArgument(getExternalLocation() != null, "withExternalLocation() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      return input
          .apply(Create.of((Void) null))
          .apply(
              SnowflakeIO.<Void, T>readAll()
                  .withDataSourceProviderFn(getDataSourceProviderFn())
                  .fromQuery(getQuery())
                  .fromTable(getTable())
                  .withCsvMapper(getCsvMapper())
                  .withExternalLocation(getExternalLocation())
                  .withIntegrationName(getIntegrationName())
                  .withCoder(getCoder()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (getQuery() != null && getQuery().get() != null) {
        builder.add(DisplayData.item("query", getQuery()));
      }
      if (getTable() != null && getTable().get() != null) {
        builder.add(DisplayData.item("table", getTable()));
      }
      builder.add(DisplayData.item("integrationName", getIntegrationName()));
      builder.add(DisplayData.item("externalLocation", getExternalLocation()));
      builder.add(DisplayData.item("csvMapper", getCsvMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  @AutoValue
  public abstract static class ReadAll<ParameterT, OutputT>
      extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {

    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract ValueProvider<String> getQuery();

    @Nullable
    abstract ValueProvider<String> getTable();

    @Nullable
    abstract ValueProvider<String> getIntegrationName();

    @Nullable
    abstract ValueProvider<String> getExternalLocation();

    @Nullable
    abstract CsvMapper<OutputT> getCsvMapper();

    @Nullable
    abstract Coder<OutputT> getCoder();

    abstract Builder<ParameterT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<ParameterT, OutputT> {
      abstract Builder<ParameterT, OutputT> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<ParameterT, OutputT> setQuery(ValueProvider<String> query);

      abstract Builder<ParameterT, OutputT> setTable(ValueProvider<String> table);

      abstract Builder<ParameterT, OutputT> setIntegrationName(
          ValueProvider<String> integrationName);

      abstract Builder<ParameterT, OutputT> setExternalLocation(
          ValueProvider<String> externalLocation);

      abstract Builder<ParameterT, OutputT> setCsvMapper(CsvMapper<OutputT> csvMapper);

      abstract Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

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
      return fromQuery(ValueProvider.StaticValueProvider.of(query));
    }

    public ReadAll<ParameterT, OutputT> fromQuery(ValueProvider<String> query) {
      return toBuilder().setQuery(query).build();
    }

    public ReadAll<ParameterT, OutputT> fromTable(String table) {
      return fromTable(ValueProvider.StaticValueProvider.of(table));
    }

    public ReadAll<ParameterT, OutputT> fromTable(ValueProvider<String> table) {
      return toBuilder().setTable(table).build();
    }

    public ReadAll<ParameterT, OutputT> withIntegrationName(String integrationName) {
      checkArgument(
          integrationName != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withIntegrationName(integrationName) called with null integrationName");
      return withIntegrationName(ValueProvider.StaticValueProvider.of(integrationName));
    }

    public ReadAll<ParameterT, OutputT> withIntegrationName(ValueProvider<String> integrationName) {
      checkArgument(
          integrationName != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withIntegrationName(integrationName) called with null integrationName");
      return toBuilder().setIntegrationName(integrationName).build();
    }

    public ReadAll<ParameterT, OutputT> withExternalLocation(String externalLocation) {
      checkArgument(
          externalLocation != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withExternalLocation(externalLocation) called with null externalLocation");
      return withExternalLocation(ValueProvider.StaticValueProvider.of(externalLocation));
    }

    public ReadAll<ParameterT, OutputT> withExternalLocation(
        ValueProvider<String> externalLocation) {
      checkArgument(
          externalLocation != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withExternalLocation(externalLocation) called with null externalLocation");
      return toBuilder().setExternalLocation(externalLocation).build();
    }

    public ReadAll<ParameterT, OutputT> withCsvMapper(CsvMapper<OutputT> csvMapper) {
      checkArgument(
          csvMapper != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withCsvMapper(csvMapper) called with null csvMapper");
      return toBuilder().setCsvMapper(csvMapper).build();
    }

    public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
      checkArgument(
          coder != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withCoder(coder) called with null coder");
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<ParameterT> input) {
      PCollection<OutputT> output;

      output =
          input
              .apply(
                  ParDo.of(
                      new CopyToExternalLocationFn<>(
                          getDataSourceProviderFn(),
                          getQuery(),
                          getTable(),
                          getIntegrationName(),
                          getExternalLocation())))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches())
              .apply(readFiles())
              .apply(ParDo.of(new MapCsvToUserDataFn<>(getCsvMapper())));

      output.setCoder(getCoder());

      try {
        TypeDescriptor<OutputT> typeDesc = getCoder().getEncodedTypeDescriptor();
        SchemaRegistry registry = input.getPipeline().getSchemaRegistry();
        Schema schema = registry.getSchema(typeDesc);
        output.setSchema(
            schema, registry.getToRowFunction(typeDesc), registry.getFromRowFunction(typeDesc));
      } catch (NoSuchSchemaException e) {
        // ignore
      }

      return output;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (getQuery() != null && getQuery().get() != null) {
        builder.add(DisplayData.item("query", getQuery()));
      }
      if (getTable() != null && getTable().get() != null) {
        builder.add(DisplayData.item("table", getTable()));
      }
      builder.add(DisplayData.item("integrationName", getIntegrationName()));
      builder.add(DisplayData.item("externalLocation", getExternalLocation()));
      builder.add(DisplayData.item("csvMapper", getCsvMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  private static class MapCsvToUserDataFn<InputT, OutputT> extends DoFn<String, OutputT> {
    private final CsvMapper<OutputT> csvMapper;

    public MapCsvToUserDataFn(CsvMapper<OutputT> csvMapper) {
      this.csvMapper = csvMapper;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      context.output(csvMapper.mapRow(context.element()));
    }
  }

  private static class CopyToExternalLocationFn<ParameterT> extends DoFn<ParameterT, String> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final ValueProvider<String> query;
    private final ValueProvider<String> table;
    private final ValueProvider<String> integrationName;
    private final ValueProvider<String> externalLocation;

    private DataSource dataSource;
    private Connection connection;

    private CopyToExternalLocationFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> query,
        ValueProvider<String> table,
        ValueProvider<String> integrationName,
        ValueProvider<String> externalLocation) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.query = query;
      this.table = table;
      this.integrationName = integrationName;
      this.externalLocation = externalLocation;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);
      connection = dataSource.getConnection();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {

      String from;
      if (query != null) {
        // Query must be surrounded with brackets
        from = String.format("(%s)", query);
      } else {
        from = table.get();
      }

      String copyQuery =
          String.format(
              "COPY INTO '%s' FROM %s STORAGE_INTEGRATION=%s FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP) OVERWRITE=true;",
              externalLocation, from, integrationName);

      runStatement(copyQuery, connection, null);

      // Replace gcs:// schema with gs:// schema
      String output = externalLocation.get().replace("gcs://", "gs://");
      // Append * because for MatchAll paths must not end with /
      output += "*";
      context.output(output);
    }

    @Teardown
    public void teardown() throws Exception {
      connection.close();
    }
  }

  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {
    @Nullable
    abstract ValueProvider<String> getUrl();

    @Nullable
    abstract ValueProvider<String> getUsername();

    @Nullable
    abstract ValueProvider<String> getPassword();

    @Nullable
    abstract ValueProvider<PrivateKey> getPrivateKey();

    @Nullable
    abstract ValueProvider<String> getOauthToken();

    @Nullable
    abstract ValueProvider<String> getDatabase();

    @Nullable
    abstract ValueProvider<String> getWarehouse();

    @Nullable
    abstract ValueProvider<String> getSchema();

    @Nullable
    abstract ValueProvider<String> getServerName();

    @Nullable
    abstract ValueProvider<Integer> getPortNumber();

    @Nullable
    abstract ValueProvider<String> getRole();

    @Nullable
    abstract ValueProvider<String> getAuthenticator();

    @Nullable
    abstract ValueProvider<Integer> getLoginTimeout();

    @Nullable
    abstract ValueProvider<Boolean> getSsl();

    @Nullable
    abstract DataSource getDataSource();

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

  @FunctionalInterface
  public interface PreparedStatementSetter<T> extends Serializable {
    void setParameters(T element, PreparedStatement preparedStatement) throws Exception;
  }

  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<String>, PCollection> {
    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract ValueProvider<String> getStage();

    @Nullable
    abstract ValueProvider<String> getTable();

    @Nullable
    abstract ValueProvider<String> getInternalLocation();

    @Nullable
    abstract ValueProvider<String> getExternalBucket();

    @Nullable
    abstract ValueProvider<String> getFileNameTemplate();

    @Nullable
    abstract ValueProvider<Boolean> getParallelization();

    @Nullable
    abstract ValueProvider<WriteDisposition> getWriteDisposition();

    @Nullable
    abstract Coder<T> getCoder();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setStage(ValueProvider<String> stage);

      abstract Builder<T> setTable(ValueProvider<String> table);

      abstract Builder<T> setInternalLocation(ValueProvider<String> filesLocation);

      abstract Builder<T> setExternalBucket(ValueProvider<String> externalBucket);

      abstract Builder<T> setFileNameTemplate(ValueProvider<String> fileNameTemplate);

      abstract Builder<T> setParallelization(ValueProvider<Boolean> parallelization);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setWriteDisposition(ValueProvider<WriteDisposition> writeDisposition);

      abstract Write<T> build();
    }

    public Write<T> withDataSourceConfiguration(final DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public Write<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public Write<T> withStage(String stage) {
      return withStage(ValueProvider.StaticValueProvider.of(stage));
    }

    public Write<T> withStage(ValueProvider<String> stage) {
      return toBuilder().setStage(stage).build();
    }

    public Write<T> withTable(String table) {
      return withTable(ValueProvider.StaticValueProvider.of(table));
    }

    public Write<T> withTable(ValueProvider<String> table) {
      return toBuilder().setTable(table).build();
    }

    public Write<T> withExternalBucket(String externalBucket) {
      return withExternalBucket(ValueProvider.StaticValueProvider.of(externalBucket));
    }

    public Write<T> withExternalBucket(ValueProvider<String> externalBucket) {
      return toBuilder().setExternalBucket(externalBucket).build();
    }

    public Write<T> withInternalLocation(String filesLocation) {
      return withInternalLocation(ValueProvider.StaticValueProvider.of(filesLocation));
    }

    public Write<T> withInternalLocation(ValueProvider<String> filesLocation) {
      return toBuilder().setInternalLocation(filesLocation).build();
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

    public Write<T> withWriteDisposition(ValueProvider<WriteDisposition> writeDisposition) {
      return toBuilder().setWriteDisposition(writeDisposition).build();
    }

    public Write<T> withWriteDisposition(WriteDisposition writeDisposition) {
      return withWriteDisposition(ValueProvider.StaticValueProvider.of(writeDisposition));
    }

    public Write<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    public enum WriteDisposition {
      TRUNCATE,
      APPEND,
      EMPTY
    }

    @Override
    public PCollection expand(PCollection<String> input) {
      checkArgument(getTable() != null, "withTable() is required");
      checkArgument(getCoder() != null, "withCoder() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      checkArgument(
          (getExternalBucket() != null || getInternalLocation() != null),
          "withExternalBucket() or withInternalLocation() is required");

      checkArgument(
          !(getExternalBucket() != null && getInternalLocation() != null),
          "withExternalBucket() or withInternalLocation() only one is required");

      ValueProvider<String> outputDirectory =
          getExternalBucket() != null ? getExternalBucket() : getInternalLocation();

      PCollection files = writeToFiles(input, outputDirectory);

      if (getExternalBucket() == null) {
        files = putFilesToInternalStage(files);
      }

      files =
          (PCollection)
              files.apply("Create list of files to copy", Combine.globally(new Concatenate()));
      PCollection out = (PCollection) files.apply("Copy files to table", copyToExternalStorage());
      out.setCoder(getCoder());
      return out;
    }

    private PCollection writeToFiles(PCollection input, ValueProvider<String> outputDirectory) {
      class Parse extends DoFn<KV<T, String>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(c.element().getValue());
        }
      }

      WriteFilesResult filesResult =
          (WriteFilesResult)
              input.apply(
                  "Write files to specified location",
                  FileIO.write()
                      .via((FileIO.Sink) new CSVSink())
                      .to(outputDirectory)
                      .withSuffix(".csv"));

      return (PCollection)
          filesResult
              .getPerDestinationOutputFilenames()
              .apply("Parse KV filenames to Strings", ParDo.of(new Parse()));
    }

    private PCollection putFilesToInternalStage(PCollection pcol) {
      if (!getParallelization().get()) {
        pcol =
            (PCollection)
                pcol.apply("Combine all files into one flow", Combine.globally(new Concatenate()));
      }

      pcol = (PCollection) pcol.apply("Put files on stage", putToInternalStage());

      pcol.setCoder(getCoder());
      return pcol;
    }

    private ParDo.SingleOutput<Object, Object> putToInternalStage() {
      return ParDo.of(
          new PutFn<>(
              getDataSourceProviderFn(),
              getStage(),
              getInternalLocation(),
              getFileNameTemplate(),
              getParallelization()));
    }

    private ParDo.SingleOutput<Object, Object> copyToExternalStorage() {
      return ParDo.of(
          new CopyLoadToBucketFn<>(
              getDataSourceProviderFn(),
              getTable(),
              getStage(),
              getExternalBucket(),
              getWriteDisposition()));
    }
  }

  private static class CopyLoadToBucketFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final ValueProvider<String> table;
    private final ValueProvider<String> stage;
    private final ValueProvider<String> externalBucket;
    private final ValueProvider<Write.WriteDisposition> writeDisposition;

    private DataSource dataSource;
    private Connection connection;

    CopyLoadToBucketFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> table,
        ValueProvider<String> stage,
        ValueProvider<String> externalBucket,
        ValueProvider<Write.WriteDisposition> writeDisposition) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.table = table;
      this.stage = stage;
      this.externalBucket = externalBucket;
      this.writeDisposition = writeDisposition;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);
      prepareTableAccordingWriteDisposition(dataSource);

      connection = dataSource.getConnection();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      List<String> filesList = (List<String>) context.element();
      String files = String.join(", ", filesList);
      files = files.replaceAll(String.valueOf(this.externalBucket), "");
      String query =
          String.format("COPY INTO %s FROM @%s FILES=(%s);", this.table, this.stage, files);
      runStatement(query, connection, null);
      context.output((OutputT) "OK");
    }

    @Teardown
    public void teardown() throws Exception {
      connection.close();
    }

    private void prepareTableAccordingWriteDisposition(DataSource dataSource) throws SQLException {
      switch (this.writeDisposition.get()) {
        case TRUNCATE:
          truncateTable(dataSource);
          break;
        case EMPTY:
          checkIfTableIsEmpty(dataSource);
          break;
        case APPEND:
        default:
          break;
      }
    }

    private void truncateTable(DataSource dataSource) throws SQLException {
      String query = String.format("TRUNCATE %s;", this.table);
      runConnectionWithStatement(dataSource, query, null);
    }

    private void checkIfTableIsEmpty(DataSource dataSource) throws SQLException {
      String selectQuery = String.format("SELECT count(*) FROM %s LIMIT 1;", this.table);
      runConnectionWithStatement(
          dataSource,
          selectQuery,
          resultSet -> {
            assert resultSet != null;
            checkIfTableIsEmpty((ResultSet) resultSet);
            return resultSet;
          });
    }

    static void checkIfTableIsEmpty(ResultSet resultSet) {
      int columnId = 1;
      try {
        if (!resultSet.next() || !checkIfTableIsEmpty(resultSet, columnId)) {
          throw new RuntimeException("Table is not empty. Aborting COPY with disposition EMPTY");
        }
      } catch (SQLException e) {
        throw new RuntimeException("Unable run pipeline with EMPTY disposition.", e);
      }
    }

    private static boolean checkIfTableIsEmpty(ResultSet resultSet, int columnId)
        throws SQLException {
      int rowCount = resultSet.getInt(columnId);
      if (rowCount >= 1) {
        return false;
        // TODO cleanup stage?
      }
      return true;
    }
  }

  private static class PutFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final ValueProvider<String> stage;
    private final ValueProvider<String> directory;
    private final ValueProvider<String> fileNameTemplate;
    private final ValueProvider<Boolean> parallelization;

    private DataSource dataSource;
    private Connection connection;

    PutFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> stage,
        ValueProvider<String> file,
        ValueProvider<String> fileNameTemplate,
        ValueProvider<Boolean> parallelization) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.stage = stage;
      this.directory = file;
      this.fileNameTemplate = fileNameTemplate;
      this.parallelization = parallelization;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);
      connection = dataSource.getConnection();
    }

    /* Right now it is paralleled per each created file */
    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      String query;
      if (parallelization.get()) {
        query = String.format("put file://%s @%s;", context.element().toString(), this.stage);
      } else {
        query =
            String.format(
                "put file://%s/%s @%s;", this.directory, this.fileNameTemplate, this.stage);
      }

      runStatement(
          query,
          connection,
          resultSet -> {
            assert resultSet != null;
            getFilenamesFromPutOperation((ResultSet) resultSet, context);
            return resultSet;
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

  private static void runConnectionWithStatement(
      DataSource dataSource, String query, Function resultSetMethod) throws SQLException {
    Connection connection = dataSource.getConnection();
    runStatement(query, connection, resultSetMethod);
    connection.close();
  }

  private static void runStatement(String query, Connection connection, Function resultSetMethod)
      throws SQLException {
    PreparedStatement statement = connection.prepareStatement(query);
    try {
      if (resultSetMethod != null) {
        ResultSet resultSet = statement.executeQuery();
        resultSetMethod.apply(resultSet);
      } else {
        statement.execute();
      }
    } finally {
      statement.close();
    }
  }
}
