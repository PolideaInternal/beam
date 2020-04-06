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
package org.apache.beam.sdk.io.snowflake.test.unit.write;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.io.snowflake.locations.LocationFactory;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeDatabase;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeServiceImpl;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.io.snowflake.test.unit.BatchTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InternalTest {
  private static final String FAKE_TABLE = "FAKE_TABLE";
  private static final String INTERNAL_LOCATION = "./bucket";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static BatchTestPipelineOptions options;
  private static SnowflakeIO.DataSourceConfiguration dc;
  private static Location locationSpec;

  private static SnowflakeService snowflakeService;
  private static List<Long> testData;

  @BeforeClass
  public static void setupAll() {
    PipelineOptionsFactory.register(BatchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BatchTestPipelineOptions.class);

    snowflakeService = new FakeSnowflakeServiceImpl();
    testData = LongStream.range(0, 100).boxed().collect(Collectors.toList());
  }

  @Before
  public void setup() {
    FakeSnowflakeDatabase.createTable(FAKE_TABLE);

    options.setInternalLocation(INTERNAL_LOCATION);
    options.setServerName("NULL.snowflakecomputing.com");

    dc =
        SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
            .withServerName(options.getServerName());
  }

  @After
  public void tearDown() {
    TestUtils.removeTempDir(INTERNAL_LOCATION);
  }

  @Test
  public void writeWithStageTest() throws SQLException {
    options.setStage("STAGE");
    locationSpec = LocationFactory.of(options);

    pipeline
        .apply(Create.of(testData))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write(snowflakeService)
                .withDataSourceConfiguration(dc)
                .to(FAKE_TABLE)
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(TestUtils.getLongCsvMapper())
                .withParallelization(false));
    pipeline.run(options).waitUntilFinish();

    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.isListsEqual(testData, actualData));
  }

  @Test
  public void writeWithNamedStageAndParalleledTest() throws SQLException {
    options.setStage("STAGE");
    locationSpec = LocationFactory.of(options);

    pipeline
        .apply(Create.of(testData))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write(snowflakeService)
                .withDataSourceConfiguration(dc)
                .to(FAKE_TABLE)
                .via(locationSpec)
                .withUserDataMapper(TestUtils.getLongCsvMapper()));

    pipeline.run(options).waitUntilFinish();

    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.isListsEqual(testData, actualData));
  }

  @Test
  public void writeWithTransformationTest() throws SnowflakeSQLException {
    options.setStage("STAGE");
    locationSpec = LocationFactory.of(options);

    String query = "select t.$1 from %s t";
    pipeline
        .apply(Create.of(testData))
        .apply(
            "External text write IO",
            SnowflakeIO.<Long>write(snowflakeService)
                .withDataSourceConfiguration(dc)
                .to(FAKE_TABLE)
                .via(locationSpec)
                .withUserDataMapper(TestUtils.getLongCsvMapper())
                .withQueryTransformation(query));

    pipeline.run(options).waitUntilFinish();

    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.isListsEqual(testData, actualData));
  }
}
