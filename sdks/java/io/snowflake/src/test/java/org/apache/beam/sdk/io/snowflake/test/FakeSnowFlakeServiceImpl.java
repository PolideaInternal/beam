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

import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.transforms.SerializableFunction;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

public class FakeSnowFlakeServiceImpl implements SnowflakeService {

    @Override
    public String executeCopyIntoStage(
            SerializableFunction<Void, DataSource> dataSourceProviderFn,
            String query,
            String table,
            String integrationName,
            String stagingBucketName,
            String tmpDirName)
            throws SQLException {

        FakeSnowFlakeDatabase database = FakeSnowFlakeDatabase.getInstance();

        writeToFile(stagingBucketName, tmpDirName, database.getTable(table));

        return String.format("./%s/%s/*", stagingBucketName, tmpDirName);
    }

    @Override
    public void executePut(
            Connection connection, // null
            String bucketName,
            String stage,
            String directory,
            String fileNameTemplate,
            Boolean parallelization, // null
            Consumer resultSetMethod // null
    ) throws SQLException {
    }

    @Override
    public void executeCopyToTable(
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
            throws SQLException {
        // write
    }


    //    TODO save into multiply files
    private void writeToFile(String stagingBucketName, String tmpDirName, List<String> rows) {
        Path filePath = Paths.get(String.format("./%s/%s/table.csv.gz", stagingBucketName,tmpDirName));
        try {
            Files.createDirectories(filePath.getParent());
            Files.createFile(filePath);
            Files.write(filePath, rows);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create files", e);
        }
    }


}
