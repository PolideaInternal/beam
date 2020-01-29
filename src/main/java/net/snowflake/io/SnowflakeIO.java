package net.snowflake.io;

import static org.apache.beam.sdk.io.TextIO.readFiles;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import net.snowflake.io.credentials.KeyPairSnowflakeCredentials;
import net.snowflake.io.credentials.OAuthTokenSnowflakeCredentials;
import net.snowflake.io.credentials.SnowflakeCredentials;
import net.snowflake.io.credentials.UsernamePasswordSnowflakeCredentials;
import net.snowflake.io.locations.Location;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeIO {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeIO.class);

  private static final String CSV_QUOTE_CHAR = "'";
  private static final String CSV_QUOTE_CHAR_FOR_COPY = "''";

  /** Read data from a Snowflake using COPY method. */
  public static <T> Read<T> read() {
    return new AutoValue_SnowflakeIO_Read.Builder<T>().build();
  }

  public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
    return new AutoValue_SnowflakeIO_ReadAll.Builder<ParameterT, OutputT>().build();
  }

  @FunctionalInterface
  public interface CsvMapper<T> extends Serializable {
    T mapRow(String[] parts) throws Exception;
  }

  @FunctionalInterface
  public interface UserDataMapper<T> extends Serializable {
    Object[] mapRow(T element);
  }

  public static <T> Write<T> write() {
    return new AutoValue_SnowflakeIO_Write.Builder<T>()
        .setFileNameTemplate(ValueProvider.StaticValueProvider.of("output*"))
        .setParallelization(ValueProvider.StaticValueProvider.of(true))
        .setCreateDisposition(
            ValueProvider.StaticValueProvider.of(Write.CreateDisposition.CREATE_IF_NEEDED))
        .setWriteDisposition(ValueProvider.StaticValueProvider.of(Write.WriteDisposition.APPEND))
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
          "net.snowflake.io.SnowflakeIO.readAll().withIntegrationName(integrationName) called with null integrationName");
      return withIntegrationName(ValueProvider.StaticValueProvider.of(integrationName));
    }

    public ReadAll<ParameterT, OutputT> withIntegrationName(ValueProvider<String> integrationName) {
      checkArgument(
          integrationName != null,
          "net.snowflake.io.SnowflakeIO.readAll().withIntegrationName(integrationName) called with null integrationName");
      return toBuilder().setIntegrationName(integrationName).build();
    }

    public ReadAll<ParameterT, OutputT> withExternalLocation(String externalLocation) {
      checkArgument(
          externalLocation != null,
          "net.snowflake.io.SnowflakeIO.readAll().withExternalLocation(externalLocation) called with null externalLocation");
      return withExternalLocation(ValueProvider.StaticValueProvider.of(externalLocation));
    }

    public ReadAll<ParameterT, OutputT> withExternalLocation(
        ValueProvider<String> externalLocation) {
      checkArgument(
          externalLocation != null,
          "net.snowflake.io.SnowflakeIO.readAll().withExternalLocation(externalLocation) called with null externalLocation");
      return toBuilder().setExternalLocation(externalLocation).build();
    }

    public ReadAll<ParameterT, OutputT> withCsvMapper(CsvMapper<OutputT> csvMapper) {
      checkArgument(
          csvMapper != null,
          "net.snowflake.io.SnowflakeIO.readAll().withCsvMapper(csvMapper) called with null csvMapper");
      return toBuilder().setCsvMapper(csvMapper).build();
    }

    public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
      checkArgument(
          coder != null,
          "net.snowflake.io.SnowflakeIO.readAll().withCoder(coder) called with null coder");
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
              .apply(ParDo.of(new MapCsvToStringArrayFn()))
              .apply(ParDo.of(new MapStringArrayToUserDataFn<>(getCsvMapper())));

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

  public static class MapCsvToStringArrayFn extends DoFn<String, String[]> {
    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      String csvLine = c.element();
      CSVParser parser = new CSVParserBuilder().withQuoteChar(CSV_QUOTE_CHAR.charAt(0)).build();
      String[] parts = parser.parseLine(csvLine);
      c.output(parts);
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
              "COPY INTO '%s' FROM %s STORAGE_INTEGRATION=%s FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_OPTIONALLY_ENCLOSED_BY='%s');",
              externalLocation, from, integrationName, CSV_QUOTE_CHAR_FOR_COPY);

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
    abstract ValueProvider<String> getTableSchema();

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

      abstract Builder<T> setTableSchema(ValueProvider<String> tableSchema);

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

    public Write<T> withTableSchema(ValueProvider<String> tableSchema) {
      return toBuilder().setTableSchema(tableSchema).build();
    }

    public Write<T> withTableSchema(String tableSchema) {
      return withTableSchema(ValueProvider.StaticValueProvider.of(tableSchema));
    }

    public enum WriteDisposition {
      TRUNCATE,
      APPEND,
      EMPTY
    }

    public enum CreateDisposition {
      CREATE_IF_NEEDED,
      CREATE_NEVER
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
                      .withSuffix(".csv"));

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
              getParallelization()));
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
              getTableSchema()));
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

  private static class MapObjecsArrayToCsvFn extends DoFn<Object[], String> {

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      List<Object> csvItems = new ArrayList<>();
      for (Object o : context.element()) {
        if (o instanceof String) {
          csvItems.add(String.format("%s%s%s", CSV_QUOTE_CHAR, o, CSV_QUOTE_CHAR));
        } else {
          csvItems.add(o);
        }
      }
      context.output(Joiner.on(",").join(csvItems));
    }
  }

  private static class CopyToTableFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final String source;
    private final String filesPath;
    private final Location location;
    private final String table;
    private final ValueProvider<String> tableSchema;
    private final ValueProvider<Write.WriteDisposition> writeDisposition;
    private final ValueProvider<Write.CreateDisposition> createDisposition;

    private DataSource dataSource;
    private Connection connection;

    CopyToTableFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> table,
        ValueProvider<String> query,
        Location location,
        ValueProvider<Write.CreateDisposition> createDisposition,
        ValueProvider<Write.WriteDisposition> writeDisposition,
        ValueProvider<String> tableSchema) {
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
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);
      connection = dataSource.getConnection();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      List<String> filesList = (List<String>) context.element();
      String files = String.join(", ", filesList);
      files = files.replaceAll(String.valueOf(this.filesPath), "");

      prepareTableAccordingCreateDisposition(dataSource);
      prepareTableAccordingWriteDisposition(dataSource);

      String query;
      if (location.isUsingIntegration()) {
        String integration = this.location.getIntegration();
        query =
            String.format(
                "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s') STORAGE_INTEGRATION=%s;",
                this.table, this.source, files, CSV_QUOTE_CHAR_FOR_COPY, integration);
      } else {
        query =
            String.format(
                "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s');",
                this.table, this.source, files, CSV_QUOTE_CHAR_FOR_COPY);
      }

      runStatement(query, connection, null);
    }

    @Teardown
    public void teardown() throws Exception {
      if (connection != null) {
        connection.close();
      }
    }

    private void prepareTableAccordingCreateDisposition(DataSource dataSource) throws SQLException {
      switch (this.createDisposition.get()) {
        case CREATE_NEVER:
          break;
        case CREATE_IF_NEEDED:
          createTableIfNotExists(dataSource);
          break;
      }
    }

    private void createTableIfNotExists(DataSource dataSource) throws SQLException {
      String query =
          String.format(
              "SELECT EXISTS (SELECT 1 FROM  information_schema.tables  WHERE  table_name = '%s');",
              table.toUpperCase());

      runConnectionWithStatement(
          dataSource,
          query,
          resultSet -> {
            assert resultSet != null;
            if (!checkResultIfTableExists((ResultSet) resultSet)) {
              try {
                createTable(dataSource);
              } catch (SQLException e) {
                throw new RuntimeException("Unable to create table.", e);
              }
            }
            return resultSet;
          });
    }

    static boolean checkResultIfTableExists(ResultSet resultSet) {
      try {
        if (resultSet.next()) {
          return checkIfResultIsTrue(resultSet);
        } else {
          throw new RuntimeException("Unable run pipeline with CREATE IF NEEDED - no response.");
        }
      } catch (SQLException e) {
        throw new RuntimeException("Unable run pipeline with CREATE IF NEEDED disposition.", e);
      }
    }

    void createTable(DataSource dataSource) throws SQLException {
      checkArgument(
          this.tableSchema != null,
          "The CREATE_IF_NEEDED disposition requires schema if table doesn't exists");
      String query = String.format("CREATE TABLE %s (%s);", this.table, this.tableSchema);
      runConnectionWithStatement(dataSource, query, null);
    }

    static boolean checkIfResultIsTrue(ResultSet resultSet) throws SQLException {
      int columnId = 1;
      return resultSet.getBoolean(columnId);
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
    private final String stage;
    private final String directory;
    private final ValueProvider<String> fileNameTemplate;
    private final ValueProvider<Boolean> parallelization;

    private DataSource dataSource;
    private Connection connection;

    PutFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        String stage,
        String file,
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
        query = String.format("put file://%s %s;", context.element().toString(), this.stage);
      } else {
        query =
            String.format(
                "put file://%s/%s %s;", this.directory, this.fileNameTemplate, this.stage);
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
