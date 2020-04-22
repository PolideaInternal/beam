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

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.snowflake.Location;
import org.apache.beam.sdk.io.snowflake.SnowflakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeCloudProvider;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeDatabase;
import org.apache.beam.sdk.io.snowflake.test.FakeSnowflakeStreamingServiceImpl;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.MatcherAssert;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingWriteTest {
  private static final String FAKE_TABLE = "TEST_TABLE";
  private static final String BUCKET_NAME = "bucket";
  private static final String SNOW_PIPE = "Snowpipe";
  private static final Instant START_TIME = new Instant(0);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException exceptionRule = ExpectedException.none();
  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static SnowflakeService snowflakeService;
  private static SnowflakeCloudProvider cloudProvider;
  private static SnowflakePipelineOptions options;
  private static List<Long> testData;
  private static Location location;

  private static final List<String> SENTENCES =
      Arrays.asList(
          "Snowflake window 1 1",
          "Snowflake window 1 2",
          "Snowflake window 1 3",
          "Snowflake window 1 4",
          "Snowflake window 2 1",
          "Snowflake window 2 2");

  private static final List<String> FIRST_WIN_WORDS = SENTENCES.subList(0, 4);
  private static final List<String> SECOND_WIN_WORDS = SENTENCES.subList(4, 6);
  private static final Duration WINDOW_DURATION = Duration.standardMinutes(1);

  @BeforeClass
  public static void setup() {
    cloudProvider = new FakeSnowflakeCloudProvider();
    snowflakeService = new FakeSnowflakeStreamingServiceImpl();

    PipelineOptionsFactory.register(SnowflakePipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SnowflakePipelineOptions.class);
    options.setStagingBucketName(BUCKET_NAME);
    options.setUsername("username");

    options.setStorageIntegration("STORAGE_INTEGRATION");

    options.setServerName("NULL.snowflakecomputing.com");

    testData = LongStream.range(0, 100).boxed().collect(Collectors.toList());

    FakeSnowflakeDatabase.createTable(FAKE_TABLE);
    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(new FakeSnowflakeBasicDataSource())
            .withServerName(options.getServerName())
            .withSchema("PUBLIC")
            .withDatabase("DATABASE")
            .withWarehouse("WAREHOUSE");

    location = Location.of(options);
  }

  @After
  public void tearDown() {
    TestUtils.removeTempDir(BUCKET_NAME);
  }

  @Test
  public void streamWriteWithOAuthFails() {
    options.setOauthToken("token");
    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withSchema("PUBLIC")
            .withDatabase("DATABASE")
            .withWarehouse("WAREHOUSE");

    exceptionRule.expectMessage("KeyPair is required for authentication");

    pipeline
        .apply(Create.of(testData))
        .apply(
            SnowflakeIO.<Long>write(snowflakeService, cloudProvider)
                .withDataSourceConfiguration(dataSourceConfiguration)
                .to(FAKE_TABLE)
                .via(location)
                .withSnowPipe(SNOW_PIPE)
                .withUserDataMapper(TestUtils.getLongCsvMapper()));

    pipeline.run(options);
  }

  @Test
  public void streamWriteWithUserPasswordFails() {
    options.setPassword("password");
    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withSchema("PUBLIC")
            .withDatabase("DATABASE")
            .withWarehouse("WAREHOUSE");

    exceptionRule.expectMessage("KeyPair is required for authentication");

    pipeline
        .apply(Create.of(testData))
        .apply(
            SnowflakeIO.<Long>write(snowflakeService, cloudProvider)
                .withDataSourceConfiguration(dataSourceConfiguration)
                .to(FAKE_TABLE)
                .via(location)
                .withSnowPipe(SNOW_PIPE)
                .withUserDataMapper(TestUtils.getLongCsvMapper()));

    pipeline.run(options);
  }

  @Test
  public void streamWriteWithKey() throws SnowflakeSQLException {
    options.setPrivateKeyPath(TestUtils.getPrivateKeyPath(getClass()));
    options.setPrivateKeyPassphrase(TestUtils.getPrivateKeyPassphrase());

    TestStream<String> stringsStream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(START_TIME)
            .addElements(event(FIRST_WIN_WORDS.get(0), 2L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(27L)))
            .addElements(
                event(FIRST_WIN_WORDS.get(1), 25L),
                event(FIRST_WIN_WORDS.get(2), 18L),
                event(FIRST_WIN_WORDS.get(3), 26L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(65L)))
            // This are late elements after window ends so they should not be saved
            .addElements(event(SECOND_WIN_WORDS.get(0), 67L), event(SECOND_WIN_WORDS.get(1), 68L))
            .advanceWatermarkToInfinity();

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withSchema("PUBLIC")
            .withDatabase("DATABASE")
            .withWarehouse("WAREHOUSE");

    pipeline
        .apply(stringsStream)
        .apply(Window.into(FixedWindows.of(WINDOW_DURATION)))
        .apply(
            SnowflakeIO.<String>write(snowflakeService, cloudProvider)
                .withDataSourceConfiguration(dataSourceConfiguration)
                .via(location)
                .withSnowPipe(SNOW_PIPE)
                .withUserDataMapper(TestUtils.getStringCsvMapper()));

    pipeline.run(options).waitUntilFinish();

    List<String> actualDataFirstWin =
        FakeSnowflakeDatabase.getElements(String.format(FAKE_TABLE)).stream()
            .map(s -> s.replaceAll("'", ""))
            .collect(Collectors.toList());

    MatcherAssert.assertThat(actualDataFirstWin, equalTo(FIRST_WIN_WORDS));
  }

  private TimestampedValue<String> event(String word, Long timestamp) {
    return TimestampedValue.of(word, START_TIME.plus(new Duration(timestamp)));
  }
}
