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
package org.apache.beam.sdk.io.snowflake.test;

import static org.apache.beam.sdk.io.snowflake.test.TestUtils.SnowflakeIOITPipelineOptions;
import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getTestRowDataMapper;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.snowflake.Location;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for streaming in CI. Has to be run with DirectRunner. Requires a snowpipe with copy into
 * STREAMING_IOIT table
 *
 * <p>Example run:
 *
 * <pre>
 * ./gradlew --info -p sdks/java/io/snowflake integrationTest -DintegrationTestPipelineOptions='[
 * "--serverName=<YOUR SNOWFLAKE SERVER NAME>",
 * "--username=<USERNAME>",
 * "--privateKeyPath=<PATH TO KEY>",
 * "--privateKeyPassphrase=<KEY PASSPHRASE>",
 * "--database=<DATABASE NAME>",
 * "--schema=<SCHEMA NAME>",
 * "--stagingBucketName=<BUCKET NAME>",
 * "--storageIntegration=<STORAGE INTEGRATION NAME>",
 * "--snowPipe=<SNOWPIPE NAME>",
 * "--runner=DirectRunner"]'
 * --tests org.apache.beam.sdk.io.snowflake.test.SnowflakeStreamingIOIT.writeStreamThenRead
 * -DintegrationTestRunner=direct
 * </pre>
 */
public class SnowflakeCIStreamingIOIT {

  private static final int EXPECTED_ROW_COUNT = 2;
  private static final int TIMEOUT = 900000;
  private static final int INTERVAL = 30000;
  private static final String TABLE = "STREAMING_IOIT";

  private static final TestRow TEST_ROW1 = TestRow.create(1, "TestRow1");
  private static final TestRow TEST_ROW2 = TestRow.create(2, "TestRow2");

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  static SnowflakeIOITPipelineOptions options;
  static SnowflakeIO.DataSourceConfiguration dc;
  static Location location;

  @BeforeClass
  public static void setupAll() throws SQLException {
    PipelineOptionsFactory.register(SnowflakeIOITPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SnowflakeIOITPipelineOptions.class);

    dc =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withDatabase(options.getDatabase())
            .withWarehouse(options.getWarehouse())
            .withSchema(options.getSchema());

    location = Location.of(options);

    TestUtils.runConnectionWithStatement(
        dc.buildDatasource(),
        String.format("CREATE OR REPLACE TABLE %s(id INTEGER, name STRING)", TABLE));
  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    TestUtils.runConnectionWithStatement(
        dc.buildDatasource(), String.format("DROP TABLE %s", TABLE));
  }

  @Test
  public void writeStreamThenRead() throws SQLException, InterruptedException {
    writeStreamToSnowflake();
    readAndVerify();
  }

  private void writeStreamToSnowflake() {

    TestStream<TestRow> stringsStream =
        TestStream.create(SerializableCoder.of(TestRow.class))
            .advanceWatermarkTo(Instant.now())
            .addElements(TEST_ROW1, TEST_ROW2)
            .advanceWatermarkToInfinity();

    pipeline
        .apply(stringsStream)
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<TestRow>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(getTestRowDataMapper())
                .withSnowPipe(options.getSnowPipe())
                .via(location));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  private void readAndVerify() throws SQLException, InterruptedException {
    int timeout = TIMEOUT;
    while (timeout > 0) {
      Set<TestRow> testRows = readDataFromStream();
      if (testRows.size() == EXPECTED_ROW_COUNT) {
        assertThat(testRows, contains(TEST_ROW1, TEST_ROW2));
        return;
      }
      Thread.sleep(INTERVAL);
      timeout -= INTERVAL;
    }
    throw new RuntimeException("Could not read data from table");
  }

  private Set<TestRow> readDataFromStream() throws SQLException {
    Connection connection = dc.buildDatasource().getConnection();
    PreparedStatement statement =
        connection.prepareStatement(String.format("SELECT * FROM %s", TABLE));
    ResultSet resultSet = statement.executeQuery();

    Set<TestRow> testRows = resultSetToJavaSet(resultSet);

    resultSet.close();
    statement.close();
    connection.close();

    return testRows;
  }

  private Set<TestRow> resultSetToJavaSet(ResultSet resultSet) throws SQLException {
    Set<TestRow> testRows = newHashSet();
    while (resultSet.next()) {
      testRows.add(TestRow.create(resultSet.getInt(1), resultSet.getString(2).replace("'", "")));
    }
    return testRows;
  }
}
