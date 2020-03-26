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
package org.apache.beam.sdk.io.snowflake.data;

import java.io.Serializable;

/** POJO describing single Column within Snowflake Table. */
public class SFColumn implements Serializable {
  private SFDataType dataType;
  private String name;
  private boolean isNull;

  public static SFColumn of(String name, SFDataType dataType) {
    return new SFColumn(name, dataType);
  }

  public static SFColumn of(String name, SFDataType dataType, boolean isNull) {
    return new SFColumn(name, dataType, isNull);
  }

  public SFColumn(String name, SFDataType dataType) {
    this.name = name;
    this.dataType = dataType;
  }

  public SFColumn(String name, SFDataType dataType, boolean isNull) {
    this.dataType = dataType;
    this.name = name;
    this.isNull = isNull;
  }

  public String sql() {
    String sql = String.format("%s %s", name, dataType.sql());
    if (isNull) {
      sql += " NULL";
    }
    return sql;
  }
}
