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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class SnowflakeServiceImpl implements SnowflakeService {

  @Override
  public void executePut(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      String bucketName,
      String stage,
      String directory,
      String fileNameTemplate,
      Boolean parallelization,
      Consumer resultSetMethod)
      throws SQLException {

    String query;
    if (parallelization) {
      query = String.format("put file://%s %s;", bucketName, stage);
    } else {
      query = String.format("put file://%s/%s %s;", directory, fileNameTemplate, stage);
    }

    runStatement(query, getConnection(dataSourceProviderFn), resultSetMethod);
  }

  @Override
  public String executeCopyIntoStage(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      String query,
      String table,
      String integrationName,
      String stagingBucketName,
      String tmpDirName)
      throws SQLException {

    String from;
    if (query != null) {
      // Query must be surrounded with brackets
      from = String.format("(%s)", query);
    } else {
      from = table;
    }

    String externalLocation = String.format("gcs://%s/%s/", stagingBucketName, tmpDirName);
    String copyQuery =
        String.format(
            "COPY INTO '%s' FROM %s STORAGE_INTEGRATION=%s FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_OPTIONALLY_ENCLOSED_BY='%s');",
            externalLocation, from, integrationName, CSV_QUOTE_CHAR_FOR_COPY);

    runStatement(copyQuery, getConnection(dataSourceProviderFn), null);

    return String.format("gs://%s/%s/*", stagingBucketName, tmpDirName);
  }

  @Override
  public void executeCopyToTable(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      List<String> filesList,
      String table,
      SFTableSchema tableSchema,
      String source,
      Location location,
      CreateDisposition createDisposition,
      WriteDisposition writeDisposition,
      String filesPath)
      throws SQLException {

    String files = String.join(", ", filesList);
    files = files.replaceAll(String.valueOf(filesPath), "");
    DataSource dataSource = dataSourceProviderFn.apply(null);

    prepareTableAccordingCreateDisposition(dataSource, table, tableSchema, createDisposition);
    prepareTableAccordingWriteDisposition(dataSource, table, writeDisposition);

    String query;
    if (location.isUsingIntegration()) {
      String integration = location.getIntegration();
      query =
          String.format(
              "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s' COMPRESSION=GZIP) STORAGE_INTEGRATION=%s;",
              table, source, files, CSV_QUOTE_CHAR_FOR_COPY, integration);
    } else {
      query =
          String.format(
              "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s' COMPRESSION=GZIP);",
              table, source, files, CSV_QUOTE_CHAR_FOR_COPY);
    }

    runStatement(query, dataSource.getConnection(), null);
  }

  private void truncateTable(DataSource dataSource, String table) throws SQLException {
    String query = String.format("TRUNCATE %s;", table);
    runConnectionWithStatement(dataSource, query, null);
  }

  private static void checkIfTableIsEmpty(DataSource dataSource, String table) throws SQLException {
    String selectQuery = String.format("SELECT count(*) FROM %s LIMIT 1;", table);
    runConnectionWithStatement(
        dataSource,
        selectQuery,
        resultSet -> {
          assert resultSet != null;
          checkIfTableIsEmpty((ResultSet) resultSet);
        });
  }

  private static void checkIfTableIsEmpty(ResultSet resultSet) {
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

  private void prepareTableAccordingCreateDisposition(
      DataSource dataSource,
      String table,
      SFTableSchema tableSchema,
      CreateDisposition createDisposition)
      throws SQLException {
    switch (createDisposition) {
      case CREATE_NEVER:
        break;
      case CREATE_IF_NEEDED:
        createTableIfNotExists(dataSource, table, tableSchema);
        break;
    }
  }

  private void prepareTableAccordingWriteDisposition(
      DataSource dataSource, String table, WriteDisposition writeDisposition) throws SQLException {
    switch (writeDisposition) {
      case TRUNCATE:
        truncateTable(dataSource, table);
        break;
      case EMPTY:
        checkIfTableIsEmpty(dataSource, table);
        break;
      case APPEND:
      default:
        break;
    }
  }

  private void createTableIfNotExists(
      DataSource dataSource, String table, SFTableSchema tableSchema) throws SQLException {
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
              createTable(dataSource, table, tableSchema);
            } catch (SQLException e) {
              throw new RuntimeException("Unable to create table.", e);
            }
          }
        });
  }

  private static boolean checkResultIfTableExists(ResultSet resultSet) {
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

  private void createTable(DataSource dataSource, String table, SFTableSchema tableSchema)
      throws SQLException {
    checkArgument(
        tableSchema != null,
        "The CREATE_IF_NEEDED disposition requires schema if table doesn't exists");
    String query = String.format("CREATE TABLE %s (%s);", table, tableSchema.sql());
    runConnectionWithStatement(dataSource, query, null);
  }

  private static boolean checkIfResultIsTrue(ResultSet resultSet) throws SQLException {
    int columnId = 1;
    return resultSet.getBoolean(columnId);
  }

  private static void runConnectionWithStatement(
      DataSource dataSource, String query, Consumer resultSetMethod) throws SQLException {
    Connection connection = dataSource.getConnection();
    runStatement(query, connection, resultSetMethod);
    connection.close();
  }

  private static void runStatement(String query, Connection connection, Consumer resultSetMethod)
      throws SQLException {
    PreparedStatement statement = connection.prepareStatement(query);
    try {
      if (resultSetMethod != null) {
        ResultSet resultSet = statement.executeQuery();
        resultSetMethod.accept(resultSet);
      } else {
        statement.execute();
      }
    } finally {
      statement.close();
      connection.close();
    }
  }

  private Connection getConnection(SerializableFunction<Void, DataSource> dataSourceProviderFn)
      throws SQLException {
    DataSource dataSource = dataSourceProviderFn.apply(null);
    return dataSource.getConnection();
  }
}
