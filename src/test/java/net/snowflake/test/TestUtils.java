package net.snowflake.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import net.snowflake.io.SnowflakeIO;

public class TestUtils {
  public static ResultSet runConnectionWithStatement(DataSource dataSource, String query)
      throws SQLException {

    Connection connection = dataSource.getConnection();
    return runStatement(query, connection);
  }

  public static ResultSet runStatement(String query, Connection connection) throws SQLException {
    PreparedStatement statement = connection.prepareStatement(query);
    try {
      return statement.executeQuery();
    } finally {
      statement.close();
      connection.close();
    }
  }

  public static SnowflakeIO.UserDataMapper<Long> getCsvMapper() {
    return (SnowflakeIO.UserDataMapper<Long>) recordLine -> new String[] {recordLine.toString()};
  }
}
