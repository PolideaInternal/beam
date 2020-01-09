package com.polidea.snowflake.io;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeIO {
  private static final long DEFAULT_BATCH_SIZE = 1000L;
  private static final int DEFAULT_FETCH_SIZE = 50_000;
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeIO.class);

  /** Read data from a JDBC datasource. */
  public static <T> Read<T> read() {
    return new AutoValue_SnowflakeIO_Read.Builder<T>()
        .setFetchSize(DEFAULT_FETCH_SIZE)
        .setOutputParallelization(true)
        .build();
  }

  public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
    return new AutoValue_SnowflakeIO_ReadAll.Builder<ParameterT, OutputT>()
        .setFetchSize(DEFAULT_FETCH_SIZE)
        .setOutputParallelization(true)
        .build();
  }

  @FunctionalInterface
  public interface RowMapper<T> extends Serializable {
    T mapRow(ResultSet resultSet) throws Exception;
  }

  @FunctionalInterface
  public interface StatementPreparator extends Serializable {
    void setParameters(PreparedStatement preparedStatement) throws Exception;
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
    abstract StatementPreparator getStatementPreparator();

    @Nullable
    abstract RowMapper<T> getRowMapper();

    @Nullable
    abstract Coder<T> getCoder();

    abstract int getFetchSize();

    abstract boolean getOutputParallelization();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setStatementPreparator(StatementPreparator statementPreparator);

      abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setFetchSize(int fetchSize);

      abstract Builder<T> setOutputParallelization(boolean outputParallelization);

      abstract Read<T> build();
    }

    public Read<T> withDataSourceConfiguration(final DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public Read<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public Read<T> withQuery(String query) {
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    public Read<T> withQuery(ValueProvider<String> query) {
      return toBuilder().setQuery(query).build();
    }

    public Read<T> withStatementPreparator(StatementPreparator statementPreparator) {
      return toBuilder().setStatementPreparator(statementPreparator).build();
    }

    public Read<T> withRowMapper(RowMapper<T> rowMapper) {
      return toBuilder().setRowMapper(rowMapper).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    public Read<T> withFetchSize(int fetchSize) {
      checkArgument(fetchSize > 0, "fetch size must be > 0");
      return toBuilder().setFetchSize(fetchSize).build();
    }

    public Read<T> withOutputParallelization(boolean outputParallelization) {
      return toBuilder().setOutputParallelization(outputParallelization).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(getQuery() != null, "withQuery() is required");
      checkArgument(getRowMapper() != null, "withRowMapper() is required");
      checkArgument(getCoder() != null, "withCoder() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      return input
          .apply(Create.of((Void) null))
          .apply(
              SnowflakeIO.<Void, T>readAll()
                  .withDataSourceProviderFn(getDataSourceProviderFn())
                  .withQuery(getQuery())
                  .withCoder(getCoder())
                  .withRowMapper(getRowMapper())
                  .withFetchSize(getFetchSize())
                  .withOutputParallelization(getOutputParallelization())
                  .withParameterSetter(
                      (element, preparedStatement) -> {
                        if (getStatementPreparator() != null) {
                          getStatementPreparator().setParameters(preparedStatement);
                        }
                      }));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", getQuery()));
      builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
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
    abstract PreparedStatementSetter<ParameterT> getParameterSetter();

    @Nullable
    abstract RowMapper<OutputT> getRowMapper();

    @Nullable
    abstract Coder<OutputT> getCoder();

    abstract int getFetchSize();

    abstract boolean getOutputParallelization();

    abstract Builder<ParameterT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<ParameterT, OutputT> {
      abstract Builder<ParameterT, OutputT> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<ParameterT, OutputT> setQuery(ValueProvider<String> query);

      abstract Builder<ParameterT, OutputT> setParameterSetter(
          PreparedStatementSetter<ParameterT> parameterSetter);

      abstract Builder<ParameterT, OutputT> setRowMapper(RowMapper<OutputT> rowMapper);

      abstract Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

      abstract Builder<ParameterT, OutputT> setFetchSize(int fetchSize);

      abstract Builder<ParameterT, OutputT> setOutputParallelization(boolean outputParallelization);

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

    public ReadAll<ParameterT, OutputT> withQuery(String query) {
      checkArgument(
          query != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withQuery(query) called with null query");
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    public ReadAll<ParameterT, OutputT> withQuery(ValueProvider<String> query) {
      checkArgument(
          query != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withQuery(query) called with null query");
      return toBuilder().setQuery(query).build();
    }

    public ReadAll<ParameterT, OutputT> withParameterSetter(
        PreparedStatementSetter<ParameterT> parameterSetter) {
      checkArgument(
          parameterSetter != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withParameterSetter(parameterSetter) called "
              + "with null statementPreparator");
      return toBuilder().setParameterSetter(parameterSetter).build();
    }

    public ReadAll<ParameterT, OutputT> withRowMapper(RowMapper<OutputT> rowMapper) {
      checkArgument(
          rowMapper != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withRowMapper(rowMapper) called with null rowMapper");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
      checkArgument(
          coder != null,
          "com.polidea.snowflake.io.SnowflakeIO.readAll().withCoder(coder) called with null coder");
      return toBuilder().setCoder(coder).build();
    }

    public ReadAll<ParameterT, OutputT> withFetchSize(int fetchSize) {
      checkArgument(fetchSize > 0, "fetch size must be >0");
      return toBuilder().setFetchSize(fetchSize).build();
    }

    public ReadAll<ParameterT, OutputT> withOutputParallelization(boolean outputParallelization) {
      return toBuilder().setOutputParallelization(outputParallelization).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<ParameterT> input) {
      PCollection<OutputT> output;
      output =
          input.apply(
              ParDo.of(
                  new ReadFn<>(
                      getDataSourceProviderFn(),
                      getQuery(),
                      getParameterSetter(),
                      getRowMapper(),
                      getFetchSize())));
      output.setCoder(getCoder());

      //      if (getOutputParallelization()) {
      //        output = output.apply(new Reparallelize<>());
      //      }

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
      builder.add(DisplayData.item("query", getQuery()));
      builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  private static class ReadFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final ValueProvider<String> query;
    private final PreparedStatementSetter<ParameterT> parameterSetter;
    private final RowMapper<OutputT> rowMapper;
    private final int fetchSize;

    private DataSource dataSource;
    private Connection connection;

    private ReadFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> query,
        PreparedStatementSetter<ParameterT> parameterSetter,
        RowMapper<OutputT> rowMapper,
        int fetchSize) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.query = query;
      this.parameterSetter = parameterSetter;
      this.rowMapper = rowMapper;
      this.fetchSize = fetchSize;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);
      connection = dataSource.getConnection();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      try (PreparedStatement statement =
          connection.prepareStatement(
              query.get(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        statement.setFetchSize(fetchSize);
        parameterSetter.setParameters(context.element(), statement);
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            context.output(rowMapper.mapRow(resultSet));
          }
        }
      }
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

    public Write<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
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
              getDataSourceProviderFn(), getTable(), getStage(), getExternalBucket()));
    }
  }

  private static class CopyLoadToBucketFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final ValueProvider<String> table;
    private final ValueProvider<String> stage;
    private final ValueProvider<String> externalBucket;

    private DataSource dataSource;
    private Connection connection;

    CopyLoadToBucketFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> table,
        ValueProvider<String> stage,
        ValueProvider<String> externalBucket) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.table = table;
      this.stage = stage;
      this.externalBucket = externalBucket;
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
      files = files.replaceAll(String.valueOf(this.externalBucket), "");
      String query =
          String.format("COPY INTO %s FROM @%s FILES=(%s);", this.table, this.stage, files);
      try (PreparedStatement statement =
          connection.prepareStatement(
              query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        try (ResultSet resultSet = statement.executeQuery()) {
          context.output((OutputT) "OK");
        }
      }
    }

    @Teardown
    public void teardown() throws Exception {
      connection.close();
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
      try (PreparedStatement statement =
          connection.prepareStatement(
              query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        try (ResultSet resultSet = statement.executeQuery()) {
          int indexOfNameOfFile = 2;
          while (resultSet.next()) {
            context.output((OutputT) resultSet.getString(indexOfNameOfFile));
          }
        }
      }
    }

    @Teardown
    public void teardown() throws Exception {
      connection.close();
    }
  }
}
