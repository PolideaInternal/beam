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
package org.apache.beam.sdk.io.snowflake.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  private static final String PRIVATE_KEY_FILE_NAME = "test_rsa_key.p8";
  private static final String PRIVATE_KEY_PASSPHRASE = "snowflake";

  public static ResultSet runConnectionWithStatement(DataSource dataSource, String query)
      throws SQLException {

    Connection connection = dataSource.getConnection();
    return runStatement(query, connection);
  }

  public static ResultSet runStatement(String query, Connection connection) throws SQLException {
    PreparedStatement statement = connection.prepareStatement(query);
    try {
      return statement.executeQuery();
    } finally {
      statement.close();
      connection.close();
    }
  }

  public static String getPrivateKeyPath(Class klass) {
    ClassLoader classLoader = klass.getClassLoader();
    File file = new File(classLoader.getResource(PRIVATE_KEY_FILE_NAME).getFile());
    return file.getAbsolutePath();
  }

  public static String getPrivateKeyPassphrase() {
    return PRIVATE_KEY_PASSPHRASE;
  }

  public static void removeTempDir(String dir) {
    Path path = Paths.get(dir);
    try (Stream<Path> stream = Files.walk(path)) {
      stream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (IOException e) {
      LOG.info("Not able to remove files");
    }
  }

  public static boolean isListsEqual(List<?> expected, List<?> actual) {
    return expected.size() == actual.size()
        && expected.containsAll(actual)
        && actual.containsAll(expected);
  }

  public static String toSnowflakeRow(String[] strings) {
    int iMax = strings.length - 1;
    StringBuilder b = new StringBuilder();
    for (int i = 0; ; i++) {
      if (strings[i] != null) {
        b.append(String.format("'%s'", strings[i]));
      }
      if (i == iMax) {
        return b.toString();
      }
      b.append(",");
    }
  }

  public static SnowflakeIO.UserDataMapper<Long> getCsvMapper() {
    return (SnowflakeIO.UserDataMapper<Long>) recordLine -> new String[] {recordLine.toString()};
  }

  public static SnowflakeIO.UserDataMapper<KV<String, Long>> getLongCsvMapperKV() {
    return (SnowflakeIO.UserDataMapper<KV<String, Long>>)
        recordLine -> new Long[] {recordLine.getValue()};
  }

  public static SnowflakeIO.UserDataMapper<Long> getLongCsvMapper() {
    return (SnowflakeIO.UserDataMapper<Long>) recordLine -> new Long[] {recordLine};
  }

  public static SnowflakeIO.UserDataMapper<String> getStringCsvMapper() {
    return (SnowflakeIO.UserDataMapper<String>) recordLine -> new String[] {recordLine};
  }

  public static SnowflakeIO.UserDataMapper<String[]> getLStringCsvMapper() {
    return (SnowflakeIO.UserDataMapper<String[]>) recordLine -> recordLine;
  }

  public static class ParseToKv extends DoFn<Long, KV<String, Long>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      KV stringIntKV = KV.of(c.element().toString(), c.element().longValue());
      c.output(stringIntKV);
    }
  }
}
