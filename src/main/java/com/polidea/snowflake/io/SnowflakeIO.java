package com.polidea.snowflake.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.jdbc.JdbcUtil;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
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
    return new Write();
  }

  public static <T> WriteVoid<T> writeVoid() {
    return new AutoValue_SnowflakeIO_WriteVoid.Builder<T>()
        .setBatchSize(DEFAULT_BATCH_SIZE)
        .setRetryStrategy(new DefaultRetryStrategy())
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

    public static DataSourceConfiguration create() {
      Builder b = new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder();
      return b.build();
    }

    public DataSourceConfiguration withUsername(String username) {
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    public DataSourceConfiguration withUsername(ValueProvider<String> username) {
      return builder().setUsername(username).build();
    }

    public DataSourceConfiguration withUrl(String url) {
      return withUrl(ValueProvider.StaticValueProvider.of(url));
    }

    public DataSourceConfiguration withUrl(ValueProvider<String> url) {
      return builder().setUrl(url).build();
    }

    public DataSourceConfiguration withPassword(String password) {
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    public DataSourceConfiguration withPassword(ValueProvider<String> password) {
      return builder().setPassword(password).build();
    }

    public DataSourceConfiguration withPrivateKey(PrivateKey privateKey) {
      return withPrivateKey(ValueProvider.StaticValueProvider.of(privateKey));
    }

    public DataSourceConfiguration withPrivateKey(ValueProvider<PrivateKey> privateKey) {
      return builder().setPrivateKey(privateKey).build();
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

    public DataSourceConfiguration withOauthToken(String oauthToken) {
      return withOauthToken(ValueProvider.StaticValueProvider.of(oauthToken));
    }

    public DataSourceConfiguration withOauthToken(ValueProvider<String> oauthToken) {
      return builder().setOauthToken(oauthToken).build();
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

    DataSource buildDatasource() {
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

  public static class Write<T> extends PTransform<PCollection<T>, PDone> {
    WriteVoid<T> inner;

    Write() {
      this(writeVoid());
    }

    Write(WriteVoid<T> inner) {
      this.inner = inner;
    }

    public Write<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      return new Write(
          inner
              .withDataSourceConfiguration(config)
              .withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config)));
    }

    public Write<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return new Write(inner.withDataSourceProviderFn(dataSourceProviderFn));
    }

    public Write<T> withStatement(String statement) {
      return new Write(inner.withStatement(statement));
    }

    public Write<T> withPreparedStatementSetter(PreparedStatementSetter<T> setter) {
      return new Write(inner.withPreparedStatementSetter(setter));
    }

    public Write<T> withBatchSize(long batchSize) {
      return new Write(inner.withBatchSize(batchSize));
    }

    public Write<T> withRetryStrategy(RetryStrategy retryStrategy) {
      return new Write(inner.withRetryStrategy(retryStrategy));
    }

    public Write<T> withTable(String table) {
      return new Write(inner.withTable(table));
    }

    /**
     * Example: write a {@link PCollection} to one database and then to another database, making
     * sure that writing a window of data to the second database starts only after the respective
     * window has been fully written to the first database.
     *
     * <pre>{@code
     * PCollection<Void> firstWriteResults = data.apply(.. .write()
     *     .withDataSourceConfiguration(CONF_DB_1).withResults());
     * data.apply(Wait.on(firstWriteResults))
     *     .apply(JdbcIO.write().withDataSourceConfiguration(CONF_DB_2));
     * }</pre>
     */
    public WriteVoid<T> withResults() {
      return inner;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      inner.populateDisplayData(builder);
    }

    private boolean hasStatementAndSetter() {
      return inner.getStatement() != null && inner.getPreparedStatementSetter() != null;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      // fixme: validate invalid table input
      if (input.hasSchema() && !hasStatementAndSetter()) {
        checkArgument(
            inner.getTable() != null, "table cannot be null if statement is not provided");
        Schema schema = input.getSchema();
        List<SchemaUtil.FieldWithIndex> fields = getFilteredFields(schema);
        inner =
            inner.withStatement(
                JdbcUtil.generateStatement(
                    inner.getTable(),
                    fields.stream()
                        .map(SchemaUtil.FieldWithIndex::getField)
                        .collect(Collectors.toList())));
        inner =
            inner.withPreparedStatementSetter(
                new AutoGeneratedPreparedStatementSetter(fields, input.getToRowFunction()));
      }

      inner.expand(input);
      return PDone.in(input.getPipeline());
    }

    private List<SchemaUtil.FieldWithIndex> getFilteredFields(Schema schema) {
      Schema tableSchema;

      try (Connection connection = inner.getDataSourceProviderFn().apply(null).getConnection();
          PreparedStatement statement =
              connection.prepareStatement((String.format("SELECT * FROM %s", inner.getTable())))) {
        tableSchema = SchemaUtil.toBeamSchema(statement.getMetaData());
        statement.close();
      } catch (SQLException e) {
        throw new RuntimeException(
            "Error while determining columns from table: " + inner.getTable(), e);
      }

      if (tableSchema.getFieldCount() < schema.getFieldCount()) {
        throw new RuntimeException("Input schema has more fields than actual table.");
      }

      // filter out missing fields from output table
      List<Schema.Field> missingFields =
          tableSchema.getFields().stream()
              .filter(
                  line ->
                      schema.getFields().stream()
                          .noneMatch(s -> s.getName().equalsIgnoreCase(line.getName())))
              .collect(Collectors.toList());

      // allow insert only if missing fields are nullable
      if (SchemaUtil.checkNullabilityForFields(missingFields)) {
        throw new RuntimeException("Non nullable fields are not allowed without schema.");
      }

      List<SchemaUtil.FieldWithIndex> tableFilteredFields =
          tableSchema.getFields().stream()
              .map(
                  (tableField) -> {
                    Optional<Schema.Field> optionalSchemaField =
                        schema.getFields().stream()
                            .filter((f) -> SchemaUtil.compareSchemaField(tableField, f))
                            .findFirst();
                    return (optionalSchemaField.isPresent())
                        ? SchemaUtil.FieldWithIndex.of(
                            tableField, schema.getFields().indexOf(optionalSchemaField.get()))
                        : null;
                  })
              .filter(Objects::nonNull)
              .collect(Collectors.toList());

      if (tableFilteredFields.size() != schema.getFieldCount()) {
        throw new RuntimeException("Provided schema doesn't match with database schema.");
      }

      return tableFilteredFields;
    }

    private class AutoGeneratedPreparedStatementSetter implements PreparedStatementSetter<T> {

      private List<SchemaUtil.FieldWithIndex> fields;
      private SerializableFunction<T, Row> toRowFn;
      private List<PreparedStatementSetCaller> preparedStatementFieldSetterList = new ArrayList<>();

      AutoGeneratedPreparedStatementSetter(
          List<SchemaUtil.FieldWithIndex> fieldsWithIndex, SerializableFunction<T, Row> toRowFn) {
        this.fields = fieldsWithIndex;
        this.toRowFn = toRowFn;
        populatePreparedStatementFieldSetter();
      }

      private void populatePreparedStatementFieldSetter() {
        IntStream.range(0, fields.size())
            .forEach(
                (index) -> {
                  Schema.FieldType fieldType = fields.get(index).getField().getType();
                  preparedStatementFieldSetterList.add(
                      (PreparedStatementSetCaller)
                          JdbcUtil.getPreparedStatementSetCaller(fieldType));
                });
      }

      @Override
      public void setParameters(T element, PreparedStatement preparedStatement) throws Exception {
        Row row = (element instanceof Row) ? (Row) element : toRowFn.apply(element);
        IntStream.range(0, fields.size())
            .forEach(
                (index) -> {
                  try {
                    preparedStatementFieldSetterList
                        .get(index)
                        .set(row, preparedStatement, index, fields.get(index));
                  } catch (SQLException | NullPointerException e) {
                    throw new RuntimeException("Error while setting data to preparedStatement", e);
                  }
                });
      }
    }
  }

  /** Interface implemented by functions that sets prepared statement data. */
  @FunctionalInterface
  interface PreparedStatementSetCaller extends Serializable {
    void set(
        Row element,
        PreparedStatement preparedStatement,
        int prepareStatementIndex,
        SchemaUtil.FieldWithIndex schemaFieldWithIndex)
        throws SQLException;
  }

  /** A {@link PTransform} to write to a JDBC datasource. */
  @AutoValue
  public abstract static class WriteVoid<T> extends PTransform<PCollection<T>, PCollection<Void>> {
    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract ValueProvider<String> getStatement();

    abstract long getBatchSize();

    @Nullable
    abstract PreparedStatementSetter<T> getPreparedStatementSetter();

    @Nullable
    abstract RetryStrategy getRetryStrategy();

    @Nullable
    abstract String getTable();

    abstract WriteVoid.Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract WriteVoid.Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract WriteVoid.Builder<T> setStatement(ValueProvider<String> statement);

      abstract WriteVoid.Builder<T> setBatchSize(long batchSize);

      abstract WriteVoid.Builder<T> setPreparedStatementSetter(PreparedStatementSetter<T> setter);

      abstract WriteVoid.Builder<T> setRetryStrategy(RetryStrategy deadlockPredicate);

      abstract WriteVoid.Builder<T> setTable(String table);

      abstract WriteVoid<T> build();
    }

    public WriteVoid<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public WriteVoid<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public WriteVoid<T> withStatement(String statement) {
      return withStatement(ValueProvider.StaticValueProvider.of(statement));
    }

    public WriteVoid<T> withStatement(ValueProvider<String> statement) {
      return toBuilder().setStatement(statement).build();
    }

    public WriteVoid<T> withPreparedStatementSetter(PreparedStatementSetter<T> setter) {
      return toBuilder().setPreparedStatementSetter(setter).build();
    }

    /**
     * Provide a maximum size in number of SQL statement for the batch. Default is 1000.
     *
     * @param batchSize maximum batch size in number of statements
     */
    public WriteVoid<T> withBatchSize(long batchSize) {
      checkArgument(batchSize > 0, "batchSize must be > 0, but was %s", batchSize);
      return toBuilder().setBatchSize(batchSize).build();
    }

    /**
     * When a SQL exception occurs, {@link Write} uses this {@link RetryStrategy} to determine if it
     * will retry the statements. If {@link RetryStrategy#apply(SQLException)} returns {@code true},
     * then {@link Write} retries the statements.
     */
    public WriteVoid<T> withRetryStrategy(RetryStrategy retryStrategy) {
      checkArgument(retryStrategy != null, "retryStrategy can not be null");
      return toBuilder().setRetryStrategy(retryStrategy).build();
    }

    public WriteVoid<T> withTable(String table) {
      checkArgument(table != null, "table name can not be null");
      return toBuilder().setTable(table).build();
    }

    @Override
    public PCollection<Void> expand(PCollection<T> input) {
      checkArgument(getStatement() != null, "withStatement() is required");
      checkArgument(
          getPreparedStatementSetter() != null, "withPreparedStatementSetter() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      return input.apply(ParDo.of(new WriteVoid.WriteFn<>(this)));
    }

    private static class WriteFn<T> extends DoFn<T, Void> {

      private final WriteVoid<T> spec;

      private static final int MAX_RETRIES = 5;
      private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RETRIES)
              .withInitialBackoff(Duration.standardSeconds(5));

      private DataSource dataSource;
      private Connection connection;
      private PreparedStatement preparedStatement;
      private final List<T> records = new ArrayList<>();

      public WriteFn(WriteVoid<T> spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        dataSource = spec.getDataSourceProviderFn().apply(null);
      }

      @StartBundle
      public void startBundle() throws Exception {
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        preparedStatement = connection.prepareStatement(spec.getStatement().get());
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        T record = context.element();

        records.add(record);

        if (records.size() >= spec.getBatchSize()) {
          executeBatch();
        }
      }

      private void processRecord(T record, PreparedStatement preparedStatement) {
        try {
          preparedStatement.clearParameters();
          spec.getPreparedStatementSetter().setParameters(record, preparedStatement);
          preparedStatement.addBatch();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        executeBatch();
        try {
          if (preparedStatement != null) {
            preparedStatement.close();
          }
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      }

      private void executeBatch() throws SQLException, IOException, InterruptedException {
        if (records.isEmpty()) {
          return;
        }
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();
        while (true) {
          try (PreparedStatement preparedStatement =
              connection.prepareStatement(spec.getStatement().get())) {
            try {
              // add each record in the statement batch
              for (T record : records) {
                processRecord(record, preparedStatement);
              }
              // execute the batch
              preparedStatement.executeBatch();
              // commit the changes
              connection.commit();
              break;
            } catch (SQLException exception) {
              if (!spec.getRetryStrategy().apply(exception)) {
                throw exception;
              }
              LOG.warn("Deadlock detected, retrying", exception);
              // clean up the statement batch and the connection state
              preparedStatement.clearBatch();
              connection.rollback();
              if (!BackOffUtils.next(sleeper, backoff)) {
                // we tried the max number of times
                throw exception;
              }
            }
          }
        }
        records.clear();
      }
    }
  }

  public static class DefaultRetryStrategy implements RetryStrategy {
    @Override
    public boolean apply(SQLException e) {
      return "40001".equals(e.getSQLState());
    }
  }

  @FunctionalInterface
  public interface RetryStrategy extends Serializable {
    boolean apply(SQLException sqlException);
  }
}
