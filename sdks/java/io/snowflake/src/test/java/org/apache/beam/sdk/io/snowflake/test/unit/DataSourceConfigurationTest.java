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
package org.apache.beam.sdk.io.snowflake.test.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.OAuthTokenSnowflakeCredentials;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link org.apache.beam.sdk.io.snowflake.SnowflakeIO.DataSourceConfiguration}. */
public class DataSourceConfigurationTest {

  private SnowflakeIO.DataSourceConfiguration configuration;

  @Before
  public void setUp() {
    configuration =
        SnowflakeIO.DataSourceConfiguration.create(
            new OAuthTokenSnowflakeCredentials("some-token"));
  }

  @Test
  public void testSettingUrlWithBadPrefix() {
    assertThrows(
        IllegalArgumentException.class,
        () -> configuration.withUrl("account.snowflakecomputing.com"));
  }

  @Test
  public void testSettingUrlWithBadSuffix() {
    assertThrows(
        IllegalArgumentException.class, () -> configuration.withUrl("jdbc:snowflake://account"));
  }

  @Test
  public void testSettingStringUrl() {
    String url = "jdbc:snowflake://account.snowflakecomputing.com";
    configuration = configuration.withUrl(url);
    assertEquals(url, configuration.getUrl().get());
  }

  @Test
  public void testSettingValueProviderUrl() {
    String url = "jdbc:snowflake://account.snowflakecomputing.com";
    ValueProvider.StaticValueProvider<String> urlVP = ValueProvider.StaticValueProvider.of(url);
    configuration = configuration.withUrl(urlVP);
    assertEquals(urlVP, configuration.getUrl());
  }

  @Test
  public void testSettingServerNameWithBadSuffix() {
    assertThrows(
        IllegalArgumentException.class, () -> configuration.withServerName("not.properly.ended"));
  }

  @Test
  public void testSettingStringServerName() {
    String serverName = "account.snowflakecomputing.com";
    configuration = configuration.withServerName(serverName);
    assertEquals(serverName, configuration.getServerName().get());
  }

  @Test
  public void testSettingValueProviderServerName() {
    String serverName = "account.snowflakecomputing.com";
    ValueProvider.StaticValueProvider<String> serverNameVP =
        ValueProvider.StaticValueProvider.of(serverName);
    configuration = configuration.withServerName(serverNameVP);
    assertEquals(serverNameVP, configuration.getServerName());
  }

  @Test
  public void testSettingStringDatabase() {
    String database = "dbname";
    configuration = configuration.withDatabase(database);
    assertEquals(database, configuration.getDatabase().get());
  }

  @Test
  public void testSettingValueProviderDatabase() {
    String database = "dbname";
    ValueProvider.StaticValueProvider<String> databaseVP =
        ValueProvider.StaticValueProvider.of(database);
    configuration = configuration.withDatabase(databaseVP);
    assertEquals(databaseVP, configuration.getDatabase());
  }

  @Test
  public void testSettingStringWarehouse() {
    String warehouse = "warehouse";
    configuration = configuration.withWarehouse(warehouse);
    assertEquals(warehouse, configuration.getWarehouse().get());
  }

  @Test
  public void testSettingValueProviderWarehouse() {
    String warehouse = "warehouse";
    ValueProvider.StaticValueProvider<String> warehouseVP =
        ValueProvider.StaticValueProvider.of(warehouse);
    configuration = configuration.withWarehouse(warehouseVP);
    assertEquals(warehouseVP, configuration.getWarehouse());
  }

  @Test
  public void testSettingStringSchema() {
    String schema = "schema";
    configuration = configuration.withSchema(schema);
    assertEquals(schema, configuration.getSchema().get());
  }

  @Test
  public void testSettingValueProviderSchema() {
    String schema = "schema";
    ValueProvider.StaticValueProvider<String> schemaVP =
        ValueProvider.StaticValueProvider.of(schema);
    configuration = configuration.withSchema(schemaVP);
    assertEquals(schemaVP, configuration.getSchema());
  }

  @Test
  public void testSettingStringRole() {
    String role = "role";
    configuration = configuration.withRole(role);
    assertEquals(role, configuration.getRole().get());
  }

  @Test
  public void testSettingValueProviderRole() {
    String role = "role";
    ValueProvider.StaticValueProvider<String> roleVP = ValueProvider.StaticValueProvider.of(role);
    configuration = configuration.withRole(roleVP);
    assertEquals(roleVP, configuration.getRole());
  }

  @Test
  public void testSettingStringAuthenticator() {
    String authenticator = "authenticator";
    configuration = configuration.withAuthenticator(authenticator);
    assertEquals(authenticator, configuration.getAuthenticator().get());
  }

  @Test
  public void testSettingValueProviderAuthenticator() {
    String authenticator = "authenticator";
    ValueProvider.StaticValueProvider<String> authenticatorVP =
        ValueProvider.StaticValueProvider.of(authenticator);
    configuration = configuration.withAuthenticator(authenticatorVP);
    assertEquals(authenticatorVP, configuration.getAuthenticator());
  }

  @Test
  public void testSettingStringPortNumber() {
    Integer portNumber = 1234;
    configuration = configuration.withPortNumber(portNumber);
    assertEquals(portNumber, configuration.getPortNumber().get());
  }

  @Test
  public void testSettingValueProviderPortNumber() {
    Integer portNumber = 1234;
    ValueProvider.StaticValueProvider<Integer> portNumberVP =
        ValueProvider.StaticValueProvider.of(portNumber);
    configuration = configuration.withPortNumber(portNumberVP);
    assertEquals(portNumberVP, configuration.getPortNumber());
  }

  @Test
  public void testSettingStringLoginTimeout() {
    Integer loginTimeout = 999;
    configuration = configuration.withLoginTimeout(loginTimeout);
    assertEquals(loginTimeout, configuration.getLoginTimeout().get());
  }

  @Test
  public void testSettingValueProviderLoginTimeout() {
    Integer loginTimeout = 999;
    ValueProvider.StaticValueProvider<Integer> loginTimeoutVP =
        ValueProvider.StaticValueProvider.of(loginTimeout);
    configuration = configuration.withLoginTimeout(loginTimeoutVP);
    assertEquals(loginTimeoutVP, configuration.getLoginTimeout());
  }

  @Test
  public void testDataSourceCreatedFromUrl() {
    String url = "jdbc:snowflake://account.snowflakecomputing.com";
    configuration = configuration.withUrl(url);

    DataSource dataSource = configuration.buildDatasource();

    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());
    assertEquals(url, ((SnowflakeBasicDataSource) dataSource).getUrl());
  }

  @Test
  public void testDataSourceCreatedFromServerName() {
    String serverName = "account.snowflakecomputing.com";
    configuration = configuration.withServerName(serverName);

    DataSource dataSource = configuration.buildDatasource();

    String expectedUrl = "jdbc:snowflake://account.snowflakecomputing.com";
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());
    assertEquals(expectedUrl, ((SnowflakeBasicDataSource) dataSource).getUrl());
  }

  @Test
  public void testDataSourceCreatedFromServerNameAndPort() {
    String serverName = "account.snowflakecomputing.com";
    int portNumber = 1234;

    configuration = configuration.withServerName(serverName);
    configuration = configuration.withPortNumber(portNumber);

    DataSource dataSource = configuration.buildDatasource();
    assertEquals(SnowflakeBasicDataSource.class, dataSource.getClass());
    String expectedUrl = "jdbc:snowflake://account.snowflakecomputing.com:1234";
    assertEquals(expectedUrl, ((SnowflakeBasicDataSource) dataSource).getUrl());
  }
}
