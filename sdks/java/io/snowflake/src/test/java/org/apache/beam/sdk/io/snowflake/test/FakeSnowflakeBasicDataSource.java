package org.apache.beam.sdk.io.snowflake.test;

import net.snowflake.client.jdbc.SnowflakeBasicDataSource;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

public class FakeSnowflakeBasicDataSource extends SnowflakeBasicDataSource implements Serializable {
    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

}
