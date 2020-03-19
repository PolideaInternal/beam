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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.data.SFColumn;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.data.text.SFVarchar;
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
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CreateDispositionWriteExternalLocationIT {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static DataSource dataSource;

  static BatchTestPipelineOptions options;
  static SnowflakeIO.DataSourceConfiguration dc;
  static Location locationSpec;

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

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
    assumeNotNull(options.getExternalLocation());
  }

  @After
  public void tearDown() throws Exception {
    if (options.getExternalLocation() != null) {
      String storageName = options.getExternalLocation();
      storageName = storageName.replaceAll("gs://", "");
      String[] splitted = storageName.split("/", 2);
      String bucketName = splitted[0];
      String path = splitted[1];
      Storage storage = StorageOptions.getDefaultInstance().getService();
      Page<Blob> blobs =
          storage.list(
              bucketName,
              Storage.BlobListOption.currentDirectory(),
              Storage.BlobListOption.prefix(path));

      for (Blob blob : blobs.iterateAll()) {
        storage.delete(blob.getBlobId());
      }
    }
  }

  @Test
  public void writeToExternalWithWriteCreateDispositionWithAlreadyCreatedTableSuccess()
      throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper())
                .withFileNameTemplate("output*")
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));
  }

  @Test
  public void writeToExternalWithWriteCreateDispositionWithCreatedTableWithoutSchemaFails() {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
        "java.lang.IllegalArgumentException: The CREATE_IF_NEEDED disposition requires schema if table doesn't exists");

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to("test_example_fail")
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeToExternalWithWriteCreateDispositionWithCreatedTableWithSchemaSuccess()
      throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    SFTableSchema tableSchema = new SFTableSchema(SFColumn.of("id", new SFVarchar()));

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to("test_example_success")
                .withTableSchema(tableSchema)
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("DROP TABLE IF EXISTS test_example_success;"));
  }

  @Test
  public void writeToExternalWithWriteCreateDispositionWithCreateNeverSuccess()
      throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));
  }

  @Test
  public void writeToExternalWithWriteCreateDispositionWithCreateNeededFails() {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
        "net.snowflake.client.jdbc.SnowflakeSQLException: SQL compilation error:"
            + "\nTable 'TEST_FAILURE' does not exist");

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to("test_failure")
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
