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
package org.apache.beam.sdk.io.snowflake.test.batch;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface BatchTestPipelineOptions extends SnowflakePipelineOptions {
  @Description("Table name to connect to.")
  String getTable();

  void setTable(String table);

  @Description("Stage name to connect to.")
  String getStage();

  void setStage(String stage);

  @Description("External location name to connect to.")
  String getExternalLocation();

  void setExternalLocation(String externalLocation);

  @Description("Internal (local) location name to connect to.")
  String getInternalLocation();

  void setInternalLocation(String internalLocation);
}
