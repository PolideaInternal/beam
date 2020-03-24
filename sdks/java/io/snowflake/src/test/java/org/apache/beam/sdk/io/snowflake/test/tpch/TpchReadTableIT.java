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
package org.apache.beam.sdk.io.snowflake.test.tpch;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link org.apache.beam.sdk.io.snowflake.SnowflakeIO} on an independent Snowflake
 * instance.
 *
 * <p>This test requires a running instance of Snowflake. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 * ./gradlew -p sdks/java/io/snowflake integrationTest -DintegrationTestPipelineOptions='[
 * "--serverName=<YOUR SNOWFLAKE SERVER NAME>",
 * "--username=<USERNAME>",
 * "--password=<PASSWORD>",
 * "--parquetFilesLocation=gs://<BUCKET-NAME>/table-parquet/",
 * "--stagingBucketName=<BUCKET-NAME>",
 * "--storageIntegration=<STORAGE INTEGRATION NAME>",
 * "--runner=DataflowRunner",
 * "--project=<GCP_PROJECT>",
 * "--testSize=TPCH_SF1000",
 * "--tempLocation=gs://<BUCKET-NAME>/dataflow-read-table-tmp"]'
 * --tests org.apache.beam.sdk.io.snowflake.test.tpch.TpchReadTableIT
 * -DintegrationTestRunner=dataflow
 * </pre>
 */
@RunWith(JUnit4.class)
public class TpchReadTableIT {
  private static final String DATABASE = "SNOWFLAKE_SAMPLE_DATA";
  private static final String TABLE = "LINEITEM";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static TpchTestPipelineOptions options;
  private static String stagingBucketName;
  private static String integrationName;
  private static String output;

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(TpchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(TpchTestPipelineOptions.class);
    stagingBucketName = options.getStagingBucketName();
    integrationName = options.getStorageIntegration();
    output = options.getParquetFilesLocation();
    String testSize = options.getTestSize();

    Assume.assumeNotNull(testSize);

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withWarehouse(options.getWarehouse())
            .withDatabase(DATABASE)
            .withSchema(testSize); // selecting tests size is done by selecting schema
  }

  @Test
  public void tpchReadTestForTable() {
    PCollection<GenericRecord> items =
        pipeline.apply(
            SnowflakeIO.<GenericRecord>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(TABLE)
                .withStagingBucketName(stagingBucketName)
                .withIntegrationName(integrationName)
                .withCsvMapper(TpchTestUtils.getCsvMapper())
                .withCoder(AvroCoder.of(TpchTestUtils.getSchema())));

    items.apply(
        FileIO.<GenericRecord>write().via(ParquetIO.sink(TpchTestUtils.getSchema())).to(output));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
