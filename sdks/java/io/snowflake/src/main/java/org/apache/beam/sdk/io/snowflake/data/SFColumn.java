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
  private boolean isNullable;

  public static SFColumn of(String name, SFDataType dataType) {
    return new SFColumn(name, dataType);
  }

  public static SFColumn of(String name, SFDataType dataType, boolean isNull) {
    return new SFColumn(name, dataType, isNull);
  }

  public SFColumn() {}

  public SFColumn(String name, SFDataType dataType) {
    this.name = name;
    this.dataType = dataType;
  }

  public SFColumn(String name, SFDataType dataType, boolean isNullable) {
    this.dataType = dataType;
    this.name = name;
    this.isNullable = isNullable;
  }

  public String sql() {
    String sql = String.format("%s %s", name, dataType.sql());
    if (isNullable) {
      sql += " NULL";
    }
    return sql;
  }

  public SFDataType getDataType() {
    return dataType;
  }

  public void setDataType(SFDataType dataType) {
    this.dataType = dataType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isNullable() {
    return isNullable;
  }

  public void setNullable(boolean nullable) {
    isNullable = nullable;
  }
}
