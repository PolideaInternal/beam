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

import static org.apache.beam.sdk.testing.SerializableMatchers.containsInAnyOrder;
import static org.junit.Assert.*;

import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.beam.sdk.io.snowflake.SnowFlakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
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

@RunWith(JUnit4.class)
public class ExternalTestIT {
    private static final String FAKE_TABLE = "FAKE_TABLE";
    private static final String EXTERNAL_LOCATION = "./bucket";

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

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


        snowflakeService = new FakeSnowflakeServiceImpl();
        cloudProvider = new FakeSnowflakeCloudProvider();
        testData = LongStream.range(0, 100).boxed().collect(Collectors.toList());
    }

    @Before
    public void setup() {
        FakeSnowflakeDatabase.createTable(FAKE_TABLE);

        options.setExternalLocation(EXTERNAL_LOCATION);
        options.setServerName("NULL.snowflakecomputing.com");

        dc =
                SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
                        .withServerName(options.getServerName());

    }

    @After
    public void tearDown(){
        TestUtils.removeDictionary(EXTERNAL_LOCATION);
    }

    @Test
    public void writeToExternalWithStageTest() throws SnowflakeSQLException {
        options.setStage("STAGE");
        locationSpec = LocationFactory.of(options);

        pipeline
                .apply(Create.of(testData))
                .apply(
                        "Write SnowflakeIO",
                        SnowflakeIO.<Long>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .withUserDataMapper(TestUtils.getLongCsvMapper())
                                .to(FAKE_TABLE)
                                .via(locationSpec));
        pipeline.run(options)
                .waitUntilFinish();

        List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

        assertTrue(TestUtils.isListsEqual(testData, actualData));

    }

    @Test
    public void writeToExternalWithIntegrationTest() throws SnowflakeSQLException {
        options.setStorageIntegration("STORAGE_INTEGRATION");
        locationSpec = LocationFactory.of(options);

        pipeline
                .apply(Create.of(testData))
                .apply(
                        "Write SnowflakeIO",
                        SnowflakeIO.<Long>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .withUserDataMapper(TestUtils.getLongCsvMapper())
                                .to(FAKE_TABLE)
                                .via(locationSpec));
        pipeline.run(options)
                .waitUntilFinish();

        List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

        assertTrue(TestUtils.isListsEqual(testData, actualData));

    }

    @Test
    public void writeToExternalWithStageWithMapperTest() throws SnowflakeSQLException {
        options.setStage("STAGE");
        locationSpec = LocationFactory.of(options);

        pipeline
                .apply(Create.of(testData))
                .apply(
                        "External text write IO",
                        SnowflakeIO.<Long>write(snowflakeService)
                                .to(FAKE_TABLE)
                                .via(locationSpec)
                                .withDataSourceConfiguration(dc)
                                .withUserDataMapper(TestUtils.getLongCsvMapper()));

        pipeline.run(options)
                .waitUntilFinish();

        List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

        assertTrue(TestUtils.isListsEqual(testData, actualData));
    }

    @Test
    public void writeToExternalWithStageKVInput() throws SnowflakeSQLException {
        options.setStage("STAGE");
        locationSpec = LocationFactory.of(options);

        pipeline
                .apply(Create.of(testData))
                .apply(ParDo.of(new ParseToKv()))
                .apply(
                        "Write SnowflakeIO",
                        SnowflakeIO.<KV<String, Long>>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .withUserDataMapper(TestUtils.getLongCsvMapperKV())
                                .to(FAKE_TABLE)
                                .via(locationSpec));

        pipeline.run(options)
                .waitUntilFinish();

    }

    @Test
    public void writeToExternalWithIntegrationWithoutStageFails(){
        options.setStorageIntegration("STORAGE_INTEGRATION");
        locationSpec = LocationFactory.of(options);

        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("withQuery() requires stage as location");

        String query = "select t.$1 from %s t";;
        SnowflakeIO.Write<Long> write =
                SnowflakeIO.<Long>write(snowflakeService)
                        .to(FAKE_TABLE)
                        .via(locationSpec)
                        .withUserDataMapper(TestUtils.getLongCsvMapper())
                        .withQueryTransformation(query)
                        .withWriteDisposition(WriteDisposition.APPEND)
                        .withDataSourceConfiguration(dc);

        write.expand(null);

    }

    @Test
    public void writeToExternalWithTransformationTest() throws SQLException {
        options.setStage("STAGE");
        locationSpec = LocationFactory.of(options);

        String query = "select t.$1 from %s t";
        pipeline
                .apply(Create.of(testData))
                .apply(ParDo.of(new ParseToKv()))
                .apply(
                        "Write SnowflakeIO",
                        SnowflakeIO.<KV<String, Long>>write(snowflakeService)
                                .to(FAKE_TABLE)
                                .via(locationSpec)
                                .withUserDataMapper(TestUtils.getLongCsvMapperKV())
                                .withDataSourceConfiguration(dc)
                                .withQueryTransformation(query));

        pipeline.run(options)
                .waitUntilFinish();

        List<Long> actualData = FakeSnowflakeDatabase.getElementsAsLong(FAKE_TABLE);

        assertTrue(TestUtils.isListsEqual(testData, actualData));
    }

    static class ParseToKv extends DoFn<Long, KV<String, Long>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV stringIntKV = KV.of(c.element().toString(), c.element().longValue());
            c.output(stringIntKV);
        }
    }

}
