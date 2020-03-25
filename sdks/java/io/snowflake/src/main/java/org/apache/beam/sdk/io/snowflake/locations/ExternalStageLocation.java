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
package org.apache.beam.sdk.io.snowflake.locations;

import java.io.Serializable;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;

/**
 * POJO Describing {@link Location} that uses <a
 * href="https://docs.snowflake.net/manuals/sql-reference/sql/create-stage.html">External Stage</a>
 * and external storage path.
 */
public class ExternalStageLocation implements Location, Serializable {
  String stage;
  String bucketPath;

  public ExternalStageLocation(SnowflakePipelineOptions options) {
    this.stage = options.getStage();
    this.bucketPath = options.getExternalLocation();
  }

  public ExternalStageLocation(String stage, String bucketPath) {
    this.stage = stage;
    this.bucketPath = bucketPath;
  }

  @Override
  public String getIntegration() {
    return "";
  }

  @Override
  public String getFilesLocationForCopy() {
    return String.format("@%s", stage);
  }

  @Override
  public Boolean isUsingIntegration() {
    return false;
  }

  @Override
  public Boolean isInternal() {
    return false;
  }

  @Override
  public String getFilesPath() {
    return this.bucketPath;
  }

  @Override
  public String getStage() {
    return stage;
  }
}
