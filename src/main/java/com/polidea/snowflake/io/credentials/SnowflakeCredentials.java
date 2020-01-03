package com.polidea.snowflake.io.credentials;

import com.polidea.snowflake.io.SnowflakeIO;

public interface SnowflakeCredentials {
  SnowflakeIO.DataSourceConfiguration createSnowflakeDataSourceConfiguration();
}
