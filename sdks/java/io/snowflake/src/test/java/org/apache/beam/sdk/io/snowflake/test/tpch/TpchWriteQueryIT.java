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
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.data.SFColumn;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFNumber;
import org.apache.beam.sdk.io.snowflake.data.text.SFVarchar;
import org.apache.beam.sdk.io.snowflake.locations.LocationFactory;
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
 * ./gradlew integrationTest -DintegrationTestPipelineOptions='[
 * "--serverName=<YOUR SNOWFLAKE SERVER NAME>",
 * "--username=<USERNAME>",
 * "--password=<PASSWORD>",
 * "--database=<DATABASE>",
 * "--table=<TABLE NAME>",
 * "--schema=<SCHEMA>",
 * "--parquetFilesLocation=gs://<BUCKET-NAME>/table-parquet/*",
 * "--externalLocation=gs://<BUCKET-NAME>/csv-query-location/",
 * "--stage=<STAGE NAME>",
 * "--runner=DataflowRunner",
 * "--project=<GCP_PROJECT>",
 * "--tempLocation=gs://<BUCKET-NAME>/dataflow-write-table-tmp"]'
 * --tests org.apache.beam.sdk.io.snowflake.test.tpch.TpchWriteQueryIT
 * -DintegrationTestRunner=direct
 * </pre>
 */
@RunWith(JUnit4.class)
public class TpchWriteQueryIT {

  private static final String QUERY_TRANSFORMATION = "select t.$1, t.$3, t.$16 from %s t";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static TpchTestPipelineOptions options;
  private static String parquetFilesLocation;
  private static String table;

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(TpchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(TpchTestPipelineOptions.class);
    parquetFilesLocation = options.getParquetFilesLocation();
    table = options.getTable();

    Assume.assumeNotNull(
        table, parquetFilesLocation, options.getExternalLocation(), options.getStage());

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withUrl(options.getUrl())
            .withServerName(options.getServerName())
            .withWarehouse(options.getWarehouse())
            .withDatabase(options.getDatabase())
            .withSchema(options.getSchema());
  }

  @Test
  public void tpchWriteTestForTable() {
    SFTableSchema tableSchema =
        SFTableSchema.of(
            SFColumn.of("L_ORDERKEY", SFNumber.of()),
            SFColumn.of("L_SUPPKEY", SFNumber.of()),
            SFColumn.of("L_COMMENT", SFVarchar.of(44)));

    PCollection<GenericRecord> items =
        pipeline.apply(ParquetIO.read(TpchTestUtils.getSchema()).from(parquetFilesLocation));

    items.apply(
        SnowflakeIO.<GenericRecord>write()
            .withDataSourceConfiguration(dataSourceConfiguration)
            .to(table)
            .withQueryTransformation(QUERY_TRANSFORMATION)
            .withCreateDisposition(SnowflakeIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withTableSchema(tableSchema)
            .via(LocationFactory.of(options))
            .withUserDataMapper(TpchTestUtils.getUserDataMapper()));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
