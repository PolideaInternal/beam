package org.apache.beam.sdk.io.snowflake.credentials;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;

public class SnowflakeCredentialsFactory {
  public static SnowflakeCredentials of(SnowflakePipelineOptions options) {
    if (options.getOauthToken() != null && !options.getOauthToken().isEmpty()) {
      return new OAuthTokenSnowflakeCredentials(options.getOauthToken());
    } else if (!options.getUsername().isEmpty() && !options.getPassword().isEmpty()) {
      return new UsernamePasswordSnowflakeCredentials(options.getUsername(), options.getPassword());
    } else if (!options.getUsername().isEmpty()
        && !options.getPrivateKeyPath().isEmpty()
        && !options.getPrivateKeyPassphrase().isEmpty()) {
      return new KeyPairSnowflakeCredentials(
          options.getUsername(), options.getPrivateKeyPath(), options.getPrivateKeyPassphrase());
    }
    throw new RuntimeException("Can't get credentials from Options");
  }
}
