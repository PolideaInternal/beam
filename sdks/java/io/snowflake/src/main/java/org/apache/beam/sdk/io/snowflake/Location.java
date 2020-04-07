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
package org.apache.beam.sdk.io.snowflake;

import java.io.Serializable;

public class Location implements Serializable {
  private String stage;
  private String storageIntegration;
  private String externalLocation;

  public Location(SnowflakePipelineOptions options) {
    this.stage = options.getStage();
    this.storageIntegration = options.getStorageIntegration();
    this.externalLocation = options.getExternalLocation();
  }

  public Location(String stage, String storageIntegration, String externalLocation) {
    this.stage = stage;
    this.storageIntegration = storageIntegration;
    this.externalLocation = externalLocation;
  }

  public String getStage() {
    return stage;
  }

  public String getStorageIntegration() {
    return storageIntegration;
  }

  public String getExternalLocation() {
    return externalLocation;
  }

  public boolean isStorageIntegration() {
    if (storageIntegration != null) {
      return true;
    }
    return false;
  }

  public String getFilesLocationForCopy() {
    if (isStorageIntegration()) {
      return String.format("'%s'", externalLocation);
    } else {
      return String.format("@%s", stage);
    }
  }
}
