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

import org.apache.beam.sdk.io.snowflake.SnowFlakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.data.SFColumn;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.data.text.SFVarchar;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.io.snowflake.locations.LocationFactory;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeDatabase;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeServiceImpl;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.io.snowflake.test.batch.BatchTestPipelineOptions;
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

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getCsvMapper;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ExternalCreateDispositionTestIT {
    public static final String FAKE_TABLE = "FAKE_TABLE";
    public static final String EXTERNAL_LOCATION = "./bucket";

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();
    @Rule public ExpectedException exceptionRule = ExpectedException.none();

    private static BatchTestPipelineOptions options;
    private static SnowflakeIO.DataSourceConfiguration dc;
    private static Location locationSpec;

    private static SnowflakeService snowflakeService;
    private static SnowFlakeCloudProvider cloudProvider;
    private static List<Long> testData;

    @BeforeClass
    public static void setupAll() {
        PipelineOptionsFactory.register(BatchTestPipelineOptions.class);
        options = TestPipeline.testingPipelineOptions().as(BatchTestPipelineOptions.class);
        options.setExternalLocation(EXTERNAL_LOCATION);
        options.setServerName("NULL.snowflakecomputing.com");
        options.setStage("STAGE");

        locationSpec = LocationFactory.of(options);

        snowflakeService = new FakeSnowflakeServiceImpl();
        cloudProvider = new FakeSnowflakeCloudProvider();
        testData = LongStream.range(0, 100).boxed().collect(Collectors.toList());

        dc =
                SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
                        .withServerName(options.getServerName());


    }

    @Before
    public void setup() {
    }

    @After
    public void tearDown() {
        TestUtils.removeDictionary(EXTERNAL_LOCATION);
        FakeSnowflakeDatabase.clean();
    }

    @Test
    public void writeToExternalWithWriteCreateDispositionWithAlreadyCreatedTableSuccess()
            throws SQLException {
        FakeSnowflakeDatabase.createTable(FAKE_TABLE);

        pipeline
                .apply(Create.of(testData))
                .apply(
                        "Copy IO",
                        SnowflakeIO.<Long>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .to(FAKE_TABLE)
                                .via(locationSpec)
                                .withUserDataMapper(TestUtils.getLongCsvMapper())
                                .withFileNameTemplate("output*")
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .withParallelization(false));

        pipeline.run(options).waitUntilFinish();

        List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

        assertTrue(TestUtils.isListsEqual(testData, actualData));


    }

    @Test
    public void writeToExternalWithWriteCreateDispositionWithCreatedTableWithoutSchemaFails() {

        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(
                "The CREATE_IF_NEEDED disposition requires schema if table doesn't exists");

        pipeline
                .apply(Create.of(testData))
                .apply(
                        "Copy IO",
                        SnowflakeIO.<Long>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .to("NO_EXIST_TABLE")
                                .via(locationSpec)
                                .withFileNameTemplate("output*")
                                .withUserDataMapper(getCsvMapper())
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .withParallelization(false));

        pipeline.run(options).waitUntilFinish();
    }


    @Test
    public void writeToExternalWithWriteCreateDispositionWithCreatedTableWithSchemaSuccess()
            throws SQLException {
        SFTableSchema tableSchema = new SFTableSchema(SFColumn.of("id", new SFVarchar()));

        pipeline
                .apply(Create.of(testData))
                .apply(
                        "Copy IO",
                        SnowflakeIO.<Long>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .to("NO_EXIST_TABLE")
                                .withTableSchema(tableSchema)
                                .via(locationSpec)
                                .withFileNameTemplate("output*")
                                .withUserDataMapper(TestUtils.getLongCsvMapper())
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .withParallelization(false));

        pipeline.run(options).waitUntilFinish();
        List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong("NO_EXIST_TABLE");

        assertTrue(TestUtils.isListsEqual(testData, actualData));
    }


    @Test
    public void writeToExternalWithWriteCreateDispositionWithCreateNeverSuccess()
            throws SQLException {
        FakeSnowflakeDatabase.createTable(FAKE_TABLE);

        pipeline
                .apply(Create.of(testData))
                .apply(
                        "Copy IO",
                        SnowflakeIO.<Long>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .to(FAKE_TABLE)
                                .via(locationSpec)
                                .withFileNameTemplate("output*")
                                .withUserDataMapper(TestUtils.getLongCsvMapper())
                                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                .withParallelization(false));

        pipeline.run(options).waitUntilFinish();
        List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

        assertTrue(TestUtils.isListsEqual(testData, actualData));

    }

    @Test
    public void writeToExternalWithWriteCreateDispositionWithCreateNeededFails() {

        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(
                "SQL compilation error: Table does not exist");

        pipeline
                .apply(Create.of(testData))
                .apply(
                        "Copy IO",
                        SnowflakeIO.<Long>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .to("NO_EXIST_TABLE")
                                .via(locationSpec)
                                .withFileNameTemplate("output*")
                                .withUserDataMapper(TestUtils.getLongCsvMapper())
                                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                .withParallelization(false));

        pipeline.run(options).waitUntilFinish();
    }

}
