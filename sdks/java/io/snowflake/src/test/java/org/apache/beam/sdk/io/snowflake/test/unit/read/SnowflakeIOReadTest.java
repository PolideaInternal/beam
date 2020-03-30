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
package org.apache.beam.sdk.io.snowflake.test.unit.read;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.snowflake.SnowFlakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeDatabase;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeServiceImpl;
import org.apache.beam.sdk.io.snowflake.test.tpch.TpchTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeIOReadTest {
  public static final String FAKE_TABLE = "FAKE_TABLE";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static TpchTestPipelineOptions options;
  private static SnowflakeService snowflakeService;
  private static SnowFlakeCloudProvider cloudProvider;
  private static FakeSnowflakeDatabase fakeSnowFlakeDatabase;

  private static String stagingBucketName;
  private static String integrationName;

  private static List<GenericRecord> avroTestData;

  @BeforeClass
  public static void setup() {

    List<String> testData = Arrays.asList("Paul,51,red", "Jackson,41,green");

    avroTestData =
        ImmutableList.of(
            new AvroGeneratedUser("Paul", 51, "red"),
            new AvroGeneratedUser("Jackson", 41, "green"));

    fakeSnowFlakeDatabase = FakeSnowflakeDatabase.getInstance();
    fakeSnowFlakeDatabase.putTable(FAKE_TABLE, testData);

    PipelineOptionsFactory.register(TpchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(TpchTestPipelineOptions.class);
    options.setServerName("NULL.snowflakecomputing.com");
    options.setStorageIntegration("STORAGE_INTEGRATION");
    options.setStagingBucketName("BUCKET");

    stagingBucketName = options.getStagingBucketName();
    integrationName = options.getStorageIntegration();

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
            .withServerName(options.getServerName());

    snowflakeService = new FakeSnowflakeServiceImpl();
    cloudProvider = new FakeSnowflakeCloudProvider();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigIsMissingStagingBucketName() {

    SnowflakeIO.Read<GenericRecord> read =
        SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema()));

    read.expand(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigIsMissingIntegrationName() {

    SnowflakeIO.Read<GenericRecord> read =
        SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema()));

    read.expand(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigIsMissingCsvMapper() {

    SnowflakeIO.Read<GenericRecord> read =
        SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema()));

    read.expand(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigIsMissingCoder() {

    SnowflakeIO.Read<GenericRecord> read =
        SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper());

    read.expand(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigIsMissingFromTableOrFromQuery() {

    SnowflakeIO.Read<GenericRecord> read =
        SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema()));

    read.expand(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigIsMissingDataSourceConfiguration() {

    SnowflakeIO.Read<GenericRecord> read =
        SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema()));

    read.expand(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigContainsFromQueryAndFromTable() {

    SnowflakeIO.Read<GenericRecord> read =
        SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromQuery("")
            .fromTable(FAKE_TABLE)
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema()));

    read.expand(null);
  }

  @Test(expected = PipelineExecutionException.class)
  public void testTableDoesntExist() {
    pipeline.apply(
        SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
            .withDataSourceConfiguration(dataSourceConfiguration)
            .fromTable("NON_EXIST")
            .withStagingBucketName(stagingBucketName)
            .withIntegrationName(integrationName)
            .withCsvMapper(getCsvMapper())
            .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    pipeline.run(options);
  }

  @Test
  public void testConfigIsProper() {
    PCollection<GenericRecord> items =
        pipeline.apply(
            SnowflakeIO.<GenericRecord>read(snowflakeService, cloudProvider)
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(FAKE_TABLE)
                .withStagingBucketName(stagingBucketName)
                .withIntegrationName(integrationName)
                .withCsvMapper(getCsvMapper())
                .withCoder(AvroCoder.of(AvroGeneratedUser.getClassSchema())));

    PAssert.that(items).containsInAnyOrder(avroTestData);
    pipeline.run(options);
  }

  static SnowflakeIO.CsvMapper<GenericRecord> getCsvMapper() {
    return (SnowflakeIO.CsvMapper<GenericRecord>)
        parts ->
            new GenericRecordBuilder(AvroGeneratedUser.getClassSchema())
                .set("name", String.valueOf(parts[0]))
                .set("favorite_number", Integer.valueOf(parts[1]))
                .set("favorite_color", String.valueOf(parts[2]))
                .build();
  }
}
