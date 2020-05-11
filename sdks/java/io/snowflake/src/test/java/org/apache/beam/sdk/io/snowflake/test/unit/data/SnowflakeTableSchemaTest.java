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
package org.apache.beam.sdk.io.snowflake.test.unit.data;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDate;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDateTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestamp;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampLTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampNTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampTZ;
import org.apache.beam.sdk.io.snowflake.data.logical.SnowflakeBoolean;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDecimal;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDouble;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeFloat;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeInteger;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumber;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumeric;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeReal;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeArray;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeObject;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeVariant;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeChar;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeString;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeText;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.junit.Test;

public class SnowflakeTableSchemaTest {
  @Test
  public void testOneColumn() {
    SnowflakeTableSchema schema =
        new SnowflakeTableSchema(SnowflakeColumn.of("id", SnowflakeVarchar.of()));

    assertEquals("id VARCHAR", schema.sql());
  }

  @Test
  public void testTwoColumns() {
    SnowflakeTableSchema schema =
        new SnowflakeTableSchema(
            SnowflakeColumn.of("id", new SnowflakeVarchar()),
            SnowflakeColumn.of("tax", new SnowflakeDouble()));

    assertEquals("id VARCHAR, tax FLOAT", schema.sql());
  }

  @Test
  public void testSerializationAndDeserializationOfDataTypes() throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    SnowflakeTableSchema schema =
        new SnowflakeTableSchema(
            new SnowflakeColumn("date", new SnowflakeDate()),
            new SnowflakeColumn("datetime", new SnowflakeDateTime()),
            new SnowflakeColumn("time", new SnowflakeTime()),
            new SnowflakeColumn("timestamp", new SnowflakeTimestamp()),
            new SnowflakeColumn("timestamp_ltz", new SnowflakeTimestampLTZ()),
            new SnowflakeColumn("timestamp_ntz", new SnowflakeTimestampNTZ()),
            new SnowflakeColumn("timestamp_tz", new SnowflakeTimestampTZ()),
            new SnowflakeColumn("boolean", new SnowflakeBoolean()),
            new SnowflakeColumn("decimal", new SnowflakeDecimal(38, 1), true),
            new SnowflakeColumn("double", new SnowflakeDouble()),
            new SnowflakeColumn("float", new SnowflakeFloat()),
            new SnowflakeColumn("integer", new SnowflakeInteger()),
            new SnowflakeColumn("number", new SnowflakeNumber(38, 1)),
            new SnowflakeColumn("numeric", new SnowflakeNumeric(40, 2)),
            new SnowflakeColumn("real", new SnowflakeReal()),
            new SnowflakeColumn("array", new SnowflakeArray()),
            new SnowflakeColumn("object", new SnowflakeObject()),
            new SnowflakeColumn("variant", new SnowflakeVariant(), true),
            new SnowflakeColumn("binary", new SnowflakeBinary()),
            new SnowflakeColumn("char", new SnowflakeChar()),
            new SnowflakeColumn("string", new SnowflakeString()),
            new SnowflakeColumn("text", new SnowflakeText()),
            new SnowflakeColumn("varbinary", new SnowflakeVarBinary()),
            new SnowflakeColumn("varchar", new SnowflakeVarchar(100)));

    String schemaString =
        "{\"schema\":["
            + "{\"dataType\":{\"type\":\"date\"},\"name\":\"date\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"datetime\"},\"name\":\"datetime\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"time\"},\"name\":\"time\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"timestamp\"},\"name\":\"timestamp\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"timestamp_ltz\"},\"name\":\"timestamp_ltz\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"timestamp_ntz\"},\"name\":\"timestamp_ntz\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"timestamp_tz\"},\"name\":\"timestamp_tz\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"boolean\"},\"name\":\"boolean\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"decimal\",\"precision\":38,\"scale\":1},\"name\":\"decimal\",\"nullable\":true},"
            + "{\"dataType\":{\"type\":\"double\"},\"name\":\"double\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"float\"},\"name\":\"float\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"integer\",\"precision\":38,\"scale\":0},\"name\":\"integer\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"number\",\"precision\":38,\"scale\":1},\"name\":\"number\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"numeric\",\"precision\":40,\"scale\":2},\"name\":\"numeric\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"real\"},\"name\":\"real\",\"nullable\":false},{\"dataType\":"
            + "{\"type\":\"array\"},\"name\":\"array\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"object\"},\"name\":\"object\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"variant\"},\"name\":\"variant\",\"nullable\":true},"
            + "{\"dataType\":{\"type\":\"binary\",\"size\":null},\"name\":\"binary\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"char\",\"length\":1},\"name\":\"char\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"string\",\"length\":null},\"name\":\"string\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"text\",\"length\":null},\"name\":\"text\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"varbinary\",\"size\":null},\"name\":\"varbinary\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"varchar\",\"length\":100},\"name\":\"varchar\",\"nullable\":false}]}";

    assertEquals(schemaString, mapper.writeValueAsString(schema));
  }
}
