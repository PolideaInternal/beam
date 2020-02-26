package org.apache.beam.sdk.io.snowflake.credentials;

import org.apache.beam.sdk.io.snowflake.SnowflakeIO;

public class OAuthTokenSnowflakeCredentials implements SnowflakeCredentials {
  private String token;

  public OAuthTokenSnowflakeCredentials(String token) {
    this.token = token;
  }

  public String getToken() {
    return token;
  }

  @Override
  public SnowflakeIO.DataSourceConfiguration createSnowflakeDataSourceConfiguration() {
    return SnowflakeIO.DataSourceConfiguration.create(this);
  }
}
