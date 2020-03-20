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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.locations.Location;

public interface SnowflakeService extends Serializable {

  void executePut(
      Connection connection,
      String bucketName,
      String stage,
      String directory,
      String fileNameTemplate,
      Boolean parallelization,
      Consumer resultSetMethod)
      throws SQLException;

  String executeCopyIntoLocation(
      Connection connection,
      String query,
      String table,
      String integrationName,
      String stagingBucketName,
      String tmpDirName)
      throws SQLException;

  void executeCopyToTable(
      Connection connection,
      List<String> filesList,
      DataSource dataSource,
      String table,
      SFTableSchema tableSchema,
      String source,
      Location location,
      CreateDisposition createDisposition,
      WriteDisposition writeDisposition,
      String filesPath)
      throws SQLException;
}
