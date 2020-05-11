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

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.snowflake.Location;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeInteger;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeString;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * A test of {@link org.apache.beam.sdk.io.snowflake.SnowflakeIO} on an independent Snowflake
 * instance.
 *
 * <p>This test requires a running instance of Snowflake, configured for your GCP. Pass in
 * connection information using PipelineOptions:
 *
 * <pre>
 * ./gradlew integrationTest -DintegrationTestPipelineOptions='[
 * "--serverName=<YOUR SNOWFLAKE SERVER NAME>",
 * "--username=<USERNAME>",
 * "--password=<PASSWORD>",
 * "--database=<DATABASE NAME>",
 * "--schema=<SCHEMA NAME>",
 * "--stagingBucketName=<GCS BUCKET NAME>",
 * "--storageIntegration=<STORAGE INTEGRATION NAME>",
 * "--numberOfRecords=<1000, 100000, 600000, 5000000>",
 * "--runner=DataflowRunner",
 * "--region=<FOR DATAFLOW RUNNER: GCP REGION>",
 * "--project=<GCP_PROJECT>"]'
 * --tests org.apache.beam.sdk.io.snowflake.test.SnowflakeIOIT
 * -DintegrationTestRunner=dataflow
 * </pre>
 */
public class SnowflakeIOIT {
  private static final String tableName = "IOIT";

  private static int numberOfRows;
  private static Location location;
  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;

  public interface SnowflakeIOITPipelineOptions
      extends IOTestPipelineOptions, SnowflakePipelineOptions {}

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    SnowflakeIOITPipelineOptions options =
        readIOTestPipelineOptions(SnowflakeIOITPipelineOptions.class);

    numberOfRows = options.getNumberOfRecords();

    location = Location.of(options);
    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withDatabase(options.getDatabase())
            .withServerName(options.getServerName())
            .withSchema(options.getSchema());
  }

  @Test
  public void testWriteThenRead() {
    PipelineResult writeResult = runWrite();
    writeResult.waitUntilFinish();

    PipelineResult readResult = runRead();
    readResult.waitUntilFinish();
  }

  @AfterClass
  public static void teardown() throws Exception {
    Storage storage = StorageOptions.getDefaultInstance().getService();
    Page<Blob> blobs =
        storage.list(location.getStagingBucketName(), Storage.BlobListOption.prefix("data"));

    for (Blob blob : blobs.iterateAll()) {
      storage.delete(blob.getBlobId());
    }

    TestUtils.runConnectionWithStatement(
        dataSourceConfiguration.buildDatasource(), String.format("DROP TABLE %s", tableName));
  }

  private PipelineResult runWrite() {

    pipelineWrite
        .apply(GenerateSequence.from(0).to(numberOfRows))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(
            SnowflakeIO.<TestRow>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withWriteDisposition(WriteDisposition.TRUNCATE)
                .withUserDataMapper(getUserDataMapper())
                .to(tableName)
                .via(location)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withTableSchema(
                    SnowflakeTableSchema.of(
                        SnowflakeColumn.of("id", SnowflakeInteger.of()),
                        SnowflakeColumn.of("name", SnowflakeString.of()))));

    return pipelineWrite.run();
  }

  private PipelineResult runRead() {
    PCollection<TestRow> namesAndIds =
        pipelineRead.apply(
            SnowflakeIO.<TestRow>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(tableName)
                .via(location)
                .withCsvMapper(getTestRowCsvMapper())
                .withCoder(SerializableCoder.of(TestRow.class)));

    PAssert.thatSingleton(namesAndIds.apply("Count All", Count.globally()))
        .isEqualTo((long) numberOfRows);

    PCollection<String> consolidatedHashcode =
        namesAndIds
            .apply(ParDo.of(new TestRow.SelectNameFn()))
            .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.that(consolidatedHashcode)
        .containsInAnyOrder(TestRow.getExpectedHashForRowCount(numberOfRows));

    return pipelineRead.run();
  }

  private SnowflakeIO.CsvMapper<TestRow> getTestRowCsvMapper() {
    return (SnowflakeIO.CsvMapper<TestRow>)
        parts -> TestRow.create(Integer.valueOf(parts[0]), parts[1]);
  }

  private SnowflakeIO.UserDataMapper getUserDataMapper() {
    return (SnowflakeIO.UserDataMapper<TestRow>)
        (TestRow element) -> new Object[] {element.id(), element.name()};
  }
}
