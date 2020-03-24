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
package org.apache.beam.sdk.io.snowflake.test.batch;

import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getCsvMapper;
import static org.junit.Assume.assumeNotNull;

import java.io.File;
import java.sql.SQLException;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.io.snowflake.locations.LocationFactory;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests that checks batch write operation of SnowflakeIO.
 * ./gradlew -p sdks/java/io/snowflake integrationTest -DintegrationTestPipelineOptions='[
 * "--runner=DirectRunner",
 * "--project=...",
 * "--storageIntegration=...",
 * "--serverName=...",
 * "--username=...",
 * "--password=...",
 * "--schema=PUBLIC",
 * "--table=...",
 * "--database=...",
 * "--stage=...",
 * "--internalLocation=gs://...",
 * "--appName=internal" ]' --tests org.apache.beam.sdk.io.snowflake.test.batch.BatchWriteInternalLocationIT -DintegrationTestRunner=direct
 */

@RunWith(JUnit4.class)
public class BatchWriteInternalLocationIT {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static DataSource dataSource;

  static BatchTestPipelineOptions options;
  static SnowflakeIO.DataSourceConfiguration dc;
  static Location locationSpec;

  @BeforeClass
  public static void setupAll() {
    PipelineOptionsFactory.register(BatchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BatchTestPipelineOptions.class);

    assumeNotNull(options.getServerName());

    dc =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withDatabase(options.getDatabase())
            .withWarehouse(options.getWarehouse())
            .withSchema(options.getSchema());

    dataSource = dc.buildDatasource();
    locationSpec = LocationFactory.of(options);
  }

  @Before
  public void setup() {
    assumeNotNull(options.getInternalLocation());
  }

  @After
  public void tearDown() throws Exception {
    if (options.getInternalLocation() != null) {
      File directory = new File(options.getInternalLocation());
      FileUtils.deleteDirectory(directory);
    }

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));
  }

  // Uses file name template which default is output*
  @Test
  public void writeWithNamedStageTest() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withParallelization(false));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  // This is more Beam way test. Parallelization is ON by default
  @Test
  public void writeWithNamedStageAndParalleledTest() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper()));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeWithTransformationTest() {
    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());

    String query = "select t.$1 from %s t";
    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "External text write IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper())
                .withQueryTransformation(query));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
