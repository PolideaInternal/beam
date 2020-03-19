package org.apache.beam.sdk.io.snowflake;

import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public interface SnowflakeService extends Serializable {

    void executeCopyIntoLocation(DoFn.ProcessContext context,
                                 Connection connection,
                                 SerializableFunction<Void, DataSource> dataSourceProviderFn,
                                 String query,
                                 String table,
                                 String integrationName,
                                 String stagingBucketName,
                                 String tmpDirName) throws SQLException;

    void executeCopyToTable(DoFn.ProcessContext context,
                            SerializableFunction<Void, DataSource> dataSourceProviderFn,
                            Connection connection,
                            DataSource dataSource,
                            String table,
                            SFTableSchema tableSchema,
                            String source,
                            Location location,
                            CreateDisposition createDisposition,
                            WriteDisposition writeDisposition,
                            String filesPath) throws SQLException;

    void executePut(DoFn.ProcessContext context,
                    Connection connection,
                    SerializableFunction<Void, DataSource> dataSourceProviderFn,
                    String stage,
                    String directory,
                    String fileNameTemplate,
                    Boolean parallelization) throws SQLException;
}
