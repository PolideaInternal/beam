package net.snowflake.io.credentials;

import net.snowflake.io.SnowflakeIO;

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
