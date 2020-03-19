package org.apache.beam.sdk.io.snowflake;

import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;


public class SnowflakeServiceImpl implements SnowflakeService {
    private static final String CSV_QUOTE_CHAR_FOR_COPY = "''";

    @Override
    public void executePut(
            DoFn.ProcessContext context,
            Connection connection,
            SerializableFunction<Void, DataSource> dataSourceProviderFn,
            String stage,
            String directory,
            String fileNameTemplate,
            Boolean parallelization) throws SQLException {
        String query;
        if (parallelization) {
            query = String.format("put file://%s %s;", context.element().toString(), stage);
        } else {
            query =
                    String.format(
                            "put file://%s/%s %s;", directory, fileNameTemplate, stage);
        }

        runStatement(
                query,
                connection,
                resultSet -> {
                    assert resultSet != null;
                    getFilenamesFromPutOperation((ResultSet) resultSet, context);
                    return resultSet;
                });
    }


    @Override
    public void executeCopyIntoLocation(DoFn.ProcessContext context,
                                        Connection connection,
                                        SerializableFunction<Void, DataSource> dataSourceProviderFn,
                                        String query,
                                        String table,
                                        String integrationName,
                                        String stagingBucketName,
                                        String tmpDirName) throws SQLException {
        String from;
        if (query != null) {
            // Query must be surrounded with brackets
            from = String.format("(%s)", query);
        } else {
            from = table;
        }

        String externalLocation =
                String.format("gcs://%s/%s/", stagingBucketName, tmpDirName);
        String copyQuery =
                String.format(
                        "COPY INTO '%s' FROM %s STORAGE_INTEGRATION=%s FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_OPTIONALLY_ENCLOSED_BY='%s');",
                        externalLocation, from, integrationName, CSV_QUOTE_CHAR_FOR_COPY);

        runStatement(copyQuery, connection, null);

        String output = String.format("gs://%s/%s/*", stagingBucketName, tmpDirName);
        context.output(output);
    }

    @Override
    public void executeCopyToTable(DoFn.ProcessContext context,
                                   SerializableFunction<Void, DataSource> dataSourceProviderFn,
                                   Connection connection,
                                   DataSource dataSource,
                                   String table,
                                   SFTableSchema tableSchema,
                                   String source,
                                   Location location,
                                   CreateDisposition createDisposition,
                                   WriteDisposition writeDisposition,
                                   String filesPath) throws SQLException {

        List<String> filesList = (List<String>) context.element();
        String files = String.join(", ", filesList);
        files = files.replaceAll(String.valueOf(filesPath), "");

        prepareTableAccordingCreateDisposition(dataSource, table, tableSchema, createDisposition);
        prepareTableAccordingWriteDisposition(dataSource, table, writeDisposition);

        String query;
        if (location.isUsingIntegration()) {
            String integration = location.getIntegration();
            query =
                    String.format(
                            "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s' COMPRESSION=GZIP) STORAGE_INTEGRATION=%s;",
                            table, source, files, CSV_QUOTE_CHAR_FOR_COPY, integration);
        } else {
            query =
                    String.format(
                            "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s' COMPRESSION=GZIP);",
                            table, source, files, CSV_QUOTE_CHAR_FOR_COPY);
        }

        runStatement(query, connection, null);
    }

    private void getFilenamesFromPutOperation(ResultSet resultSet, DoFn.ProcessContext context) {
        int indexOfNameOfFile = 2;
        try {
            while (resultSet.next()) {
//                TODO add (OutputT) before (resultSet
                context.output(resultSet.getString(indexOfNameOfFile));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Unable run pipeline with PUT operation.", e);
        }
    }

    private void truncateTable(DataSource dataSource, String table) throws SQLException {
        String query = String.format("TRUNCATE %s;", table);
        runConnectionWithStatement(dataSource, query, null);
    }

    private static void checkIfTableIsEmpty(DataSource dataSource, String table) throws SQLException {
        String selectQuery = String.format("SELECT count(*) FROM %s LIMIT 1;", table);
        runConnectionWithStatement(
                dataSource,
                selectQuery,
                resultSet -> {
                    assert resultSet != null;
                    checkIfTableIsEmpty((ResultSet) resultSet);
                    return resultSet;
                });
    }

    private static void checkIfTableIsEmpty(ResultSet resultSet) {
        int columnId = 1;
        try {
            if (!resultSet.next() || !checkIfTableIsEmpty(resultSet, columnId)) {
                throw new RuntimeException("Table is not empty. Aborting COPY with disposition EMPTY");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Unable run pipeline with EMPTY disposition.", e);
        }
    }

    private static boolean checkIfTableIsEmpty(ResultSet resultSet, int columnId)
            throws SQLException {
        int rowCount = resultSet.getInt(columnId);
        if (rowCount >= 1) {
            return false;
            // TODO cleanup stage?
        }
        return true;
    }

    private void prepareTableAccordingCreateDisposition(DataSource dataSource, String table, SFTableSchema tableSchema, CreateDisposition createDisposition) throws SQLException {
        switch (createDisposition) {
            case CREATE_NEVER:
                break;
            case CREATE_IF_NEEDED:
                createTableIfNotExists(dataSource, table, tableSchema);
                break;
        }
    }

    private void prepareTableAccordingWriteDisposition(DataSource dataSource, String table, WriteDisposition writeDisposition) throws SQLException {
        switch (writeDisposition) {
            case TRUNCATE:
                truncateTable(dataSource, table);
                break;
            case EMPTY:
                checkIfTableIsEmpty(dataSource, table);
                break;
            case APPEND:
            default:
                break;
        }
    }

    private void createTableIfNotExists(DataSource dataSource, String table, SFTableSchema tableSchema) throws SQLException {
        String query =
                String.format(
                        "SELECT EXISTS (SELECT 1 FROM  information_schema.tables  WHERE  table_name = '%s');",
                        table.toUpperCase());

        runConnectionWithStatement(
                dataSource,
                query,
                resultSet -> {
                    assert resultSet != null;
                    if (!checkResultIfTableExists((ResultSet) resultSet)) {
                        try {
                            createTable(dataSource, table, tableSchema);
                        } catch (SQLException e) {
                            throw new RuntimeException("Unable to create table.", e);
                        }
                    }
                    return resultSet;
                });
    }

    private static boolean checkResultIfTableExists(ResultSet resultSet) {
        try {
            if (resultSet.next()) {
                return checkIfResultIsTrue(resultSet);
            } else {
                throw new RuntimeException("Unable run pipeline with CREATE IF NEEDED - no response.");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Unable run pipeline with CREATE IF NEEDED disposition.", e);
        }
    }

    private void createTable(DataSource dataSource, String table, SFTableSchema tableSchema) throws SQLException {
        checkArgument(
                tableSchema != null,
                "The CREATE_IF_NEEDED disposition requires schema if table doesn't exists");
        String query =
                String.format("CREATE TABLE %s (%s);", table, tableSchema.sql());
        runConnectionWithStatement(dataSource, query, null);
    }

    private static boolean checkIfResultIsTrue(ResultSet resultSet) throws SQLException {
        int columnId = 1;
        return resultSet.getBoolean(columnId);
    }

    private static void runConnectionWithStatement(
            DataSource dataSource, String query, Function resultSetMethod) throws SQLException {
        Connection connection = dataSource.getConnection();
        runStatement(query, connection, resultSetMethod);
        connection.close();
    }

    private static void runStatement(String query, Connection connection, Function resultSetMethod)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        try {
            if (resultSetMethod != null) {
                ResultSet resultSet = statement.executeQuery();
                resultSetMethod.apply(resultSet);
            } else {
                statement.execute();
            }
        } finally {
            statement.close();
        }
    }


}
