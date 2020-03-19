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

import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.options.ValueProvider;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public interface SnowflakeService extends Serializable {

    void executePut(
            String bucketName,
            Connection connection,
            String stage,
            String directory,
            ValueProvider<String> fileNameTemplate,
            ValueProvider<Boolean> parallelization,
            Consumer resultSetMethod)
            throws SQLException;

    String executeCopyIntoLocation(
            Connection connection,
            ValueProvider<String> query,
            ValueProvider<String> table,
            ValueProvider<String> integrationName,
            ValueProvider<String> stagingBucketName,
            ValueProvider<String> tmpDirName)
            throws SQLException;

    void executeCopyToTable(
            List<String> filesList,
            Connection connection,
            DataSource dataSource,
            String table,
            ValueProvider<SFTableSchema> tableSchema,
            String source,
            Location location,
            ValueProvider<CreateDisposition> createDisposition,
            ValueProvider<WriteDisposition> writeDisposition,
            String filesPath)
            throws SQLException;

}
