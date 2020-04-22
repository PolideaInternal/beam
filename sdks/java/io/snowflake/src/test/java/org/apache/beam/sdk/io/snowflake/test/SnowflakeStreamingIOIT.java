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
package org.apache.beam.sdk.io.snowflake.test;

import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getStringCsvMapper;
import static org.junit.Assume.assumeNotNull;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.snowflake.Location;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/**
 * A test of {@link org.apache.beam.sdk.io.snowflake.test.SnowflakeStreamingIOIT} on an independent
 * Snowflake instance.
 *
 * <p>This test requires a running instance of Snowflake, configured for your GCP. Pass in
 * connection information using PipelineOptions:
 *
 * <pre>
 * ./gradlew integrationTest -DintegrationTestPipelineOptions='[
 * "--serverName=<YOUR SNOWFLAKE SERVER NAME>",
 * "--username=<USERNAME>",
 * "--privateKeyPath=<PATH_TO_KEY>",
 * "--privateKeyPassphrase=<KEY_PASSPHRASE>",
 * "--database=<DATABASE NAME>",
 * "--schema=<SCHEMA NAME>",
 * "--stagingBucketName=<BUCKET-NAME>",
 * "--storageIntegration=<STORAGE INTEGRATION NAME>",
 * "--runner=DataflowRunner",
 * "--project=<GCP_PROJECT>"]'
 * --tests org.apache.beam.sdk.io.snowflake.test.SnowflakeIOIT
 * -DintegrationTestRunner=dataflow
 * </pre>
 */
public class SnowflakeStreamingIOIT {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  static SnowflakeIOITPipelineOptions options;
  static SnowflakeIO.DataSourceConfiguration dc;
  static Location location;

  public interface SnowflakeIOITPipelineOptions
      extends IOTestPipelineOptions, SnowflakePipelineOptions {}

  @BeforeClass
  public static void setupAll() {
    PipelineOptionsFactory.register(SnowflakeIOITPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SnowflakeIOITPipelineOptions.class);

    assumeNotNull(options.getServerName());

    dc =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withDatabase(options.getDatabase())
            .withWarehouse(options.getWarehouse())
            .withSchema(options.getSchema());

    location = Location.of(options);
  }

  @Test
  @Ignore
  public void writeStreamOfSequence() {
    pipeline
        .apply(GenerateSequence.from(0))
        .apply(
            Window.<Long>into(FixedWindows.of(Duration.millis(1000)))
                .withAllowedLateness(Duration.ZERO)
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(100)))
                .discardingFiredPanes())
        .apply(ToString.elements())
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(getStringCsvMapper())
                .withSnowPipe(options.getSnowPipe())
                .via(location));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeFromPubSubToSnowflake() {
    pipeline
        .apply(
            PubsubIO.readStrings()
                .fromTopic("projects/pubsub-public-data/topics/taxirides-realtime"))
        .apply(
            Window.<String>into(FixedWindows.of(Duration.millis(100)))
                .withAllowedLateness(Duration.ZERO)
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2000)))
                .discardingFiredPanes())
        .apply(ToString.elements())
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(getStreamingCsvMapper())
                .withSnowPipe(options.getSnowPipe())
                .via(location));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  public static SnowflakeIO.UserDataMapper<String> getStreamingCsvMapper() {
    return (SnowflakeIO.UserDataMapper<String>)
        recordLine -> {
          JsonParser jsonParser = new JsonParser();
          JsonObject jo = (JsonObject) jsonParser.parse(recordLine);

          String[] strings = {
            jo.get("ride_id").toString(),
            jo.get("latitude").toString(),
            jo.get("longitude").toString()
          };

          return strings;
        };
  }
}
