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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
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
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class QueryDispositionInternalLocationTest {
  private static final String FAKE_TABLE = "FAKE_TABLE";
  private static final String INTERNAL_LOCATION = "./bucket";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

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

    options.setInternalLocation(INTERNAL_LOCATION);
    options.setServerName("NULL.snowflakecomputing.com");
    options.setStage("STAGE");
    locationSpec = LocationFactory.of(options);

    dc =
        SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
            .withServerName(options.getServerName());
  }

  @After
  public void tearDown() {
    TestUtils.removeTempDir(INTERNAL_LOCATION);
  }

  @Test
  public void writeWithWriteTruncateDispositionSuccess() throws SQLException {

    FakeSnowflakeDatabase.createTableWithElements(FAKE_TABLE, Arrays.asList("NOT_EMPTY"));

    pipeline
        .apply(Create.of(testData))
        .apply(
            "Truncate before write",
            SnowflakeIO.<Long>write(snowflakeService)
                .withDataSourceConfiguration(dc)
                .to(FAKE_TABLE)
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(TestUtils.getLongCsvMapper())
                .withWriteDisposition(WriteDisposition.TRUNCATE)
                .withParallelization(false));
    pipeline.run(options).waitUntilFinish();

    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.isListsEqual(testData, actualData));
  }

  @Test
  public void writeWithWriteEmptyDispositionWithNotEmptyTableFails() throws SQLException {
    FakeSnowflakeDatabase.createTableWithElements(FAKE_TABLE, Arrays.asList("NOT_EMPTY"));

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
        "java.lang.RuntimeException: Table is not empty. Aborting COPY with disposition EMPTY");

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
                .withWriteDisposition(WriteDisposition.EMPTY)
                .withParallelization(false));

    pipeline.run(options).waitUntilFinish();
  }

  @Test
  public void writeWithWriteEmptyDispositionWithEmptyTableSuccess() throws SQLException {
    FakeSnowflakeDatabase.createTable(FAKE_TABLE);

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
                .withWriteDisposition(WriteDisposition.EMPTY)
                .withParallelization(false));

    pipeline.run(options).waitUntilFinish();

    List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

    assertTrue(TestUtils.isListsEqual(testData, actualData));
  }
}
