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
package org.apache.beam.sdk.io.snowflake.xlang;

import org.apache.beam.sdk.io.snowflake.credentials.KeyPairSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.OAuthTokenSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.UsernamePasswordSnowflakeCredentials;

public class XlangUtils {
  public static SnowflakeCredentials createCredentials(Configuration config) {
    if (config.getOAuthToken() != null) {
      return new OAuthTokenSnowflakeCredentials(config.getOAuthToken());
    } else if (config.getPrivateKeyFile() != null
        && config.getPrivateKeyPassword() != null
        && config.getUsername() != null) {
      return new KeyPairSnowflakeCredentials(
          config.getUsername(), config.getPrivateKeyFile(), config.getPrivateKeyPassword());
    } else if (config.getUsername() != null && config.getPassword() != null) {
      return new UsernamePasswordSnowflakeCredentials(config.getUsername(), config.getPassword());
    } else {
      throw new RuntimeException("No credentials given");
    }
  }
}
