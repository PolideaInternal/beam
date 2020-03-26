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

import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.snowflake.SfCloudProvider;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.test.FakeSFCloudProvider;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowFlakeDatabase;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowFlakeServiceImpl;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowFlakeIOReadTest {
  public static final String FAKE_TABLE = "FAKE_TABLE";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static TpchTestPipelineOptions options;
  private static SnowflakeService snowflakeService;
  private static SfCloudProvider cloudProvider;
  private static FakeSnowFlakeDatabase fakeSnowFlakeDatabase;

  private static String stagingBucketName;
  private static String integrationName;

  @BeforeClass
  public static void setup() {

    List<String> rows =
        Arrays.asList(
            "3628897,108036,8037,1,8.00,8352.24,0.04,0.00,'N','O','1996-09-16','1996-07-26','1996-10-01','TAKE BACK RETURN','TRUCK','uctions play car'",
            "3628897,145958,5959,2,19.00,38075.05,0.05,0.03,'N','O','1996-10-05','1996-08-10','1996-10-26','DELIVER IN PERSON','MAIL','ges boost. pending instruction',");

    fakeSnowFlakeDatabase = FakeSnowFlakeDatabase.getInstance();
    fakeSnowFlakeDatabase.putTable(FAKE_TABLE, rows);

    PipelineOptionsFactory.register(TpchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(TpchTestPipelineOptions.class);
    options.setUsername("NULL");
    options.setPassword("NULL");
    //            options.setRunner(DirectRunner.class);
    options.setServerName("NULL.snowflakecomputing.com");
    options.setStorageIntegration("STORAGE_INTEGRATION");
    options.setStagingBucketName("BUCKET");
    options.setTempLocation("NULL");

    stagingBucketName = options.getStagingBucketName();
    integrationName = options.getStorageIntegration();

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withSnowflakeBasicDataSource(new FakeSnowflakeBasicDataSource())
            .withServerName(options.getServerName());

    snowflakeService = new FakeSnowFlakeServiceImpl();
    cloudProvider = new FakeSFCloudProvider();
  }

  @Test
  public void tpchReadTestForTable() {
    PCollection<GenericRecord> items =
        pipeline.apply(
            SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(FAKE_TABLE)
                .withStagingBucketName(stagingBucketName)
                .withIntegrationName(integrationName)
                .withCsvMapper(TpchTestUtils.getCsvMapper())
                .withCoder(AvroCoder.of(TpchTestUtils.getSchema())));

    PAssert.that(items).containsInAnyOrder();
    pipeline.run(options);
    System.out.println();
  }
}
