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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import javax.sql.DataSource;

import org.apache.beam.sdk.io.snowflake.RunStatementResult;
import org.apache.beam.sdk.io.snowflake.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.transforms.SerializableFunction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 * Fake implementation of {@link org.apache.beam.sdk.io.snowflake.SnowflakeService} used in tests.
 */
public class FakeSnowflakeServiceImpl implements SnowflakeService {

    @Override
    public String copyIntoStage(
            SerializableFunction<Void, DataSource> dataSourceProviderFn,
            String query,
            String table,
            String integrationName,
            String stagingBucketName,
            String tmpDirName)
            throws SQLException {

        writeToFile(stagingBucketName, tmpDirName, FakeSnowflakeDatabase.getElements(table));

        return String.format("./%s/%s/*", stagingBucketName, tmpDirName);
    }

    @Override
    public void putOnStage(
            SerializableFunction<Void, DataSource> dataSourceProviderFn,
            String bucketName,
            String stage,
            String directory,
            String fileNameTemplate,
            Boolean parallelization,
            Consumer<RunStatementResult> runStatementResultConsumer)
            throws SQLException {

        RunStatementResult<String> runStatementResult = new RunStatementResult<>();
        Arrays.asList(bucketName.replace("[", "").replace("]", "").replaceAll(" ", "").split(","))
                .forEach(file -> runStatementResult.add(file));

        runStatementResultConsumer.accept(runStatementResult);
    }

    @Override
    public void copyToTable(
            SerializableFunction<Void, DataSource> dataSourceProviderFn,
            List<String> filesList,
            String table,
            SFTableSchema tableSchema,
            String source,
            Location location,
            CreateDisposition createDisposition,
            WriteDisposition writeDisposition,
            String filesPath)
            throws SQLException {

        List<String> rows = new ArrayList<>();
        for (String file : filesList) {
            rows.addAll(readGZIPFile(file.replace("'", "")));
        }

        prepareTableAccordingCreateDisposition(table, tableSchema, createDisposition);
//        prepareTableAccordingWriteDisposition(dataSource, table, writeDisposition);

//    prepareTableAccordingCreateDisposition(dataSource, table, tableSchema, createDisposition);
//    prepareTableAccordingWriteDisposition(dataSource, table, writeDisposition);

//    String query;
//    if (location.isUsingIntegration()) {
//      String integration = location.getIntegration();
//      query =
//              String.format(
//                      "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s' COMPRESSION=GZIP) STORAGE_INTEGRATION=%s;",
//                      table, source, files, CSV_QUOTE_CHAR_FOR_COPY, integration);
//    } else {
//      query =
//              String.format(
//                      "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s' COMPRESSION=GZIP);",
//                      table, source, files, CSV_QUOTE_CHAR_FOR_COPY);
//    }

        FakeSnowflakeDatabase.createTableWithElements(table, rows);

    }

    private void prepareTableAccordingCreateDisposition(
            String table,
            SFTableSchema tableSchema,
            CreateDisposition createDisposition)
            throws SQLException {
        switch (createDisposition) {
            case CREATE_NEVER:
                break;
            case CREATE_IF_NEEDED:
                if (FakeSnowflakeDatabase.isTableExist(table)) {
                    break;
                } else if (tableSchema == null) {
                    throw new RuntimeException("The CREATE_IF_NEEDED disposition requires schema if table doesn't exists");
                } else {
                    FakeSnowflakeDatabase.createTable(table);
                }

                break;
        }
    }

//    private void prepareTableAccordingWriteDisposition(
//            DataSource dataSource, String table, WriteDisposition writeDisposition) throws SQLException {
//        switch (writeDisposition) {
//            case TRUNCATE:
////                truncateTable(dataSource, table);
////                remove elements from tabel
//
//                break;
//            case EMPTY:
//                checkIfTableIsEmpty(dataSource, table);
//                break;
//            case APPEND:
//            default:
//                break;
//        }
//    }

    private void writeToFile(String stagingBucketName, String tmpDirName, List<String> rows) {
        Path filePath = Paths.get(String.format("./%s/%s/table.csv.gz", stagingBucketName, tmpDirName));
        try {
            Files.createDirectories(filePath.getParent());
            Files.createFile(filePath);
            Files.write(filePath, rows);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create files", e);
        }
    }

    private List<String> readGZIPFile(String file) {
        List<String> lines = new ArrayList<>();
        try {
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file));
            BufferedReader br = new BufferedReader(new InputStreamReader(gzip, Charset.defaultCharset()));

            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file", e);
        }

        return lines;
    }
}
