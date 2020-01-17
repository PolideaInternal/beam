package net.snowflake.io.credentials;

import net.snowflake.io.SnowflakeIO;

public interface SnowflakeCredentials {
  SnowflakeIO.DataSourceConfiguration createSnowflakeDataSourceConfiguration();
}
