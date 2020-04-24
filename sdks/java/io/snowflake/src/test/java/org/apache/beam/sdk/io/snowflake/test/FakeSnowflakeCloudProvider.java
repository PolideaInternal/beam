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

import java.io.Serializable;
import org.apache.beam.sdk.io.snowflake.SnowflakeCloudProvider;
import org.apache.commons.lang3.RandomStringUtils;

/** Fake implementation of {@link SnowflakeCloudProvider} used in test code. */
public class FakeSnowflakeCloudProvider implements SnowflakeCloudProvider, Serializable {
  private static final String SF_PREFIX = "gs://";

  @Override
  public void removeFiles(String stagingBucketName, String pathOnBucket) {
    TestUtils.removeTempDir(stagingBucketName);
  }

  @Override
  public String formatCloudPath(String... pathSteps) {
    StringBuilder builder = new StringBuilder();
    builder.append("./");

    for (String step : pathSteps) {
      builder.append(String.format("%s/", step));
    }

    return builder.toString();
  }

  @Override
  public String transformSnowflakePathToCloudPath(String path) {
    return path.replace(SF_PREFIX, "./");
  }

  @Override
  public String transformCloudPathToSnowflakePath(String path) {
    return path;
  }

  @Override
  public String createCloudStoragePath(String stagingBucketName) {
    String writeTmpPath = String.format("ioit_tmp_%s", RandomStringUtils.randomAlphanumeric(16));
    return String.format("./%s/%s", stagingBucketName, writeTmpPath);
  }
}
