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

import java.util.*;

/** Fake implementation of SnowFlake warehouse used in test code */
public class FakeSnowFlakeDatabase {
  private static FakeSnowFlakeDatabase instance = null;

  private Map<String, List<String>> tables;

  private FakeSnowFlakeDatabase() {
    tables = new HashMap<>();
  }

  public static FakeSnowFlakeDatabase getInstance() {
    if (instance == null) instance = new FakeSnowFlakeDatabase();
    return instance;
  }

  public FakeSnowFlakeDatabase(Map<String, List<String>> tables) {
    this.tables = new HashMap<>();
  }

  public Map<String, List<String>> getTables() {
    return tables;
  }

  public List<String> getTable(String table) {
    return this.tables.get(table);
  }

  public List<String> putTable(String table, List<String> rows) {
    return this.tables.put(table, rows);
  }
}
