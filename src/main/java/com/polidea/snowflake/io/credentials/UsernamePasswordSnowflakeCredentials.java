package com.polidea.snowflake.io.credentials;

public class UsernamePasswordSnowflakeCredentials implements SnowflakeCredentials {
  private String username;
  private String password;

  public UsernamePasswordSnowflakeCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
