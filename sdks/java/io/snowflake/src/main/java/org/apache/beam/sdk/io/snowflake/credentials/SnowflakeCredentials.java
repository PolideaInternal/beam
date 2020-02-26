package org.apache.beam.sdk.io.snowflake.credentials;

import org.apache.beam.sdk.io.snowflake.SnowflakeIO;

public interface SnowflakeCredentials {
  SnowflakeIO.DataSourceConfiguration createSnowflakeDataSourceConfiguration();
}
