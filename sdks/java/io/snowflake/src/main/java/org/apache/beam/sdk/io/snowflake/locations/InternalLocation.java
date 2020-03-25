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
 * href="https://docs.snowflake.net/manuals/sql-reference/sql/create-stage.html">Internal Stage</a>.
 */
public class InternalLocation implements Location, Serializable {
  String stage;
  String filesLocation;

  public InternalLocation(SnowflakePipelineOptions options) {
    this.stage = options.getStage();
    this.filesLocation = options.getInternalLocation();
  }

  public InternalLocation(String stage, String internalLocation) {
    this.stage = stage;
    this.filesLocation = internalLocation;
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
  public String getIntegration() {
    return "";
  }

  @Override
  public Boolean isInternal() {
    return true;
  }

  @Override
  public String getFilesPath() {
    return this.filesLocation;
  }

  @Override
  public String getStage() {
    return stage;
  }
}
