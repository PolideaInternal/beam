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
  private String storageIntegration;
  private String externalLocation;

  public static Location of(SnowflakePipelineOptions options) {
    return new Location(options.getStorageIntegration(), options.getExternalLocation());
  }

  public static Location of(String storageIntegration, String externalLocation) {
    return new Location(storageIntegration, externalLocation);
  }

  private Location(String storageIntegration, String externalLocation) {
    this.storageIntegration = storageIntegration;
    this.externalLocation = externalLocation;
  }

  public String getStorageIntegration() {
    return storageIntegration;
  }

  public String getExternalLocation() {
    return externalLocation;
  }

  public boolean isStorageIntegration() {
    return storageIntegration != null;
  }

  public String getFilesLocationForCopy() {
    return String.format("'%s'", externalLocation);
  }
}
