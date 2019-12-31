package com.polidea.snowflake.io.credentials;

public class OAuthTokenSnowflakeCredentials implements SnowflakeCredentials {
  private String token;

  public OAuthTokenSnowflakeCredentials(String token) {
    this.token = token;
  }

  public String getToken() {
    return token;
  }
}
