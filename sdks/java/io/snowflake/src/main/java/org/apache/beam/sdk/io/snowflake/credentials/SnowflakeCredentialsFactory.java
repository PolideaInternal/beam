/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.snowflake.credentials;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;

/**
 * Factory class for creating implementations of {@link SnowflakeCredentials} from {@link
 * SnowflakePipelineOptions}
 */
public class SnowflakeCredentialsFactory {
  public static SnowflakeCredentials of(SnowflakePipelineOptions options) {
    if (options.getOauthToken() != null && !options.getOauthToken().isEmpty()) {
      return new OAuthTokenSnowflakeCredentials(options.getOauthToken());
    } else if (options.getUsername() != null && !options.getUsername().isEmpty() && !options.getPassword().isEmpty()) {
      return new UsernamePasswordSnowflakeCredentials(options.getUsername(), options.getPassword());
    } else if (options.getUsername() != null && !options.getUsername().isEmpty()
        && !options.getPrivateKeyPath().isEmpty()
        && !options.getPrivateKeyPassphrase().isEmpty()) {
      return new KeyPairSnowflakeCredentials(
          options.getUsername(), options.getPrivateKeyPath(), options.getPrivateKeyPassphrase());
    }
    throw new RuntimeException("Can't get credentials from Options");
  }
}
