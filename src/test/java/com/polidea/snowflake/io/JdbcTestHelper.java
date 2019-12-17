package com.polidea.snowflake.io;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class JdbcTestHelper {

  static class CreateTestRowOfNameAndId implements SnowflakeIO.RowMapper {
    @Override
    public TestRow mapRow(ResultSet resultSet) throws Exception {
      return TestRow.create(resultSet.getInt("ID"), resultSet.getString("NAME"));
    }
  }

  static class PrepareStatementFromTestRow implements SnowflakeIO.PreparedStatementSetter<TestRow> {
    @Override
    public void setParameters(TestRow element, PreparedStatement statement) throws SQLException {
      statement.setInt(1, element.id());
      statement.setString(2, element.name());
    }
  }
}
