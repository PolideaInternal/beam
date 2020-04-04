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

import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.beam.sdk.io.snowflake.SnowFlakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.data.SFColumn;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFDate;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFDateTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTime;
import org.apache.beam.sdk.io.snowflake.data.structured.SFArray;
import org.apache.beam.sdk.io.snowflake.data.structured.SFObject;
import org.apache.beam.sdk.io.snowflake.data.structured.SFVariant;
import org.apache.beam.sdk.io.snowflake.data.text.SFText;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ExternalSchemaDispositionTestIT {
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

//    static class GenerateNulls extends DoFn<Long, String[]> {
//        @ProcessElement
//        public void processElement(ProcessContext c) {
//
//            String[] listOfDates = {null, null, null};
//            c.output(listOfDates);
//        }
//    }

//    static class GenerateStructuredData extends DoFn<Long, String[]> {
//        @ProcessElement
//        public void processElement(ProcessContext c) {
//            String json = "{ \"key1\": 1, \"key2\": {\"inner_key\": \"value2\", \"inner_key2\":18} }";
//            String array = "[1,2,3]";
//            String[] listOfDates = {array, json, json};
//            c.output(listOfDates);
//        }
//    }

    public static SnowflakeIO.UserDataMapper<String[]> getCsvMapper() {
        return (SnowflakeIO.UserDataMapper<String[]>) recordLine -> recordLine;
    }

    @Test
    public void writeToExternalWithCreatedTableWithDatetimeSchemaSuccess() throws SQLException {

        List<String[]> testDates = LongStream.range(0, 100).boxed().map(num -> new String []{"2020-08-25", "2014-01-01 16:00:00", "00:02:03"}).collect(Collectors.toList());
        List<String> testDatesSnowFlakeFormat = testDates.stream().map(TestUtils::toSnowFlakeRow).collect(Collectors.toList());

        SFTableSchema tableSchema =
                new SFTableSchema(
                        SFColumn.of("date", SFDate.of()),
                        SFColumn.of("datetime", SFDateTime.of()),
                        SFColumn.of("time", SFTime.of()));

        pipeline
                .apply(Create.of(testDates))
                .apply(
                        "Copy IO",
                        SnowflakeIO.<String[]>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .to("NO_EXIST_TABLE")
                                .withTableSchema(tableSchema)
                                .via(locationSpec)
                                .withFileNameTemplate("output*")
                                .withUserDataMapper(TestUtils.getLStringCsvMapper())
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .withParallelization(false));

        pipeline.run(options).waitUntilFinish();


        List<String> actualData = FakeSnowflakeDatabase.getElements("NO_EXIST_TABLE");
        assertTrue(TestUtils.isListsEqual(testDatesSnowFlakeFormat, actualData));
    }

    @Test
    public void writeToExternalWithCreatedTableWithNullValuesInSchemaSuccess() throws SnowflakeSQLException {

        List<String[]> testNulls = LongStream.range(0, 100).boxed().map(num -> new String []{null, null, null}).collect(Collectors.toList());
        List<String> testNullsSnowFlakeFormat = testNulls.stream().map(TestUtils::toSnowFlakeRow).collect(Collectors.toList());

        SFTableSchema tableSchema =
                new SFTableSchema(
                        SFColumn.of("date", SFDate.of(), true),
                        new SFColumn("datetime", SFDateTime.of(), true),
                        SFColumn.of("text", SFText.of(), true));

        pipeline
                .apply(Create.of(testNulls))
                .apply(
                        "Copy IO",
                        SnowflakeIO.<String[]>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .to("NO_EXIST_TABLE")
                                .withTableSchema(tableSchema)
                                .via(locationSpec)
                                .withFileNameTemplate("output*")
                                .withUserDataMapper(getCsvMapper())
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .withParallelization(false));

        pipeline.run(options).waitUntilFinish();

        List<String> actualData = FakeSnowflakeDatabase.getElements("NO_EXIST_TABLE");
        assertTrue(TestUtils.isListsEqual(testNullsSnowFlakeFormat, actualData));

    }

    @Test
    public void writeToExternalWithCreatedTableWithStructuredDataSchemaSuccess() throws SQLException {
        String json = "{ \"key1\": 1, \"key2\": {\"inner_key\": \"value2\", \"inner_key2\":18} }";
        String array = "[1,2,3]";

        List<String[]> testStructuredData = LongStream.range(0, 100).boxed().map(num -> new String []{json, array, json}).collect(Collectors.toList());
        List<String> testStructuredDataSnowFlakeFormat = testStructuredData.stream().map(TestUtils::toSnowFlakeRow).collect(Collectors.toList());

        SFTableSchema tableSchema =
                new SFTableSchema(
                        SFColumn.of("variant", SFArray.of()),
                        SFColumn.of("object", SFObject.of()),
                        SFColumn.of("array", SFVariant.of()));

        pipeline
                .apply(Create.of(testStructuredData))
                .apply(
                        "Copy IO",
                        SnowflakeIO.<String[]>write(snowflakeService)
                                .withDataSourceConfiguration(dc)
                                .to("NO_EXIST_TABLE")
                                .withTableSchema(tableSchema)
                                .via(locationSpec)
                                .withFileNameTemplate("output*")
                                .withUserDataMapper(getCsvMapper())
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .withParallelization(false));

        pipeline.run(options).waitUntilFinish();

        List<String> actualData = FakeSnowflakeDatabase.getElements("NO_EXIST_TABLE");
        assertTrue(TestUtils.isListsEqual(testStructuredDataSnowFlakeFormat, actualData));
    }


}
