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
import org.apache.beam.sdk.io.snowflake.data.SFColumn;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFDate;
import org.apache.beam.sdk.io.snowflake.data.logical.SFBoolean;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFDecimal;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFDouble;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFFloat;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFInteger;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFNumber;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFNumeric;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFReal;
import org.apache.beam.sdk.io.snowflake.data.structured.SFArray;
import org.apache.beam.sdk.io.snowflake.data.structured.SFObject;
import org.apache.beam.sdk.io.snowflake.data.structured.SFVariant;
import org.apache.beam.sdk.io.snowflake.data.text.SFBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SFChar;
import org.apache.beam.sdk.io.snowflake.data.text.SFString;
import org.apache.beam.sdk.io.snowflake.data.text.SFText;
import org.apache.beam.sdk.io.snowflake.data.text.SFVarBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SFVarchar;
import org.junit.Test;

public class SFTableSchemaTest {
  @Test
  public void testOneColumn() {
    SFTableSchema schema = new SFTableSchema(SFColumn.of("id", SFVarchar.of()));

    assertEquals("id VARCHAR", schema.sql());
  }

  @Test
  public void testTwoColumns() {
    SFTableSchema schema =
        new SFTableSchema(SFColumn.of("id", new SFVarchar()), SFColumn.of("tax", new SFDouble()));

    assertEquals("id VARCHAR, tax FLOAT", schema.sql());
  }

  @Test
  public void testSerializationAndDeserializationOfDataTypes() throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    SFTableSchema schema =
        new SFTableSchema(
            new SFColumn("date", new SFDate()),
            new SFColumn("date", new SFDate()),
            new SFColumn("date", new SFDate()),
            new SFColumn("date", new SFDate()),
            new SFColumn("date", new SFDate()),
            new SFColumn("date", new SFDate()),
            new SFColumn("date", new SFDate()),
            new SFColumn("boolean", new SFBoolean()),
            new SFColumn("decimal", new SFDecimal(38, 1), true),
            new SFColumn("double", new SFDouble()),
            new SFColumn("float", new SFFloat()),
            new SFColumn("integer", new SFInteger()),
            new SFColumn("number", new SFNumber(38, 1)),
            new SFColumn("numeric", new SFNumeric(40, 2)),
            new SFColumn("real", new SFReal()),
            new SFColumn("array", new SFArray()),
            new SFColumn("object", new SFObject()),
            new SFColumn("variant", new SFVariant(), true),
            new SFColumn("binary", new SFBinary()),
            new SFColumn("char", new SFChar()),
            new SFColumn("string", new SFString()),
            new SFColumn("text", new SFText()),
            new SFColumn("varbinary", new SFVarBinary()),
            new SFColumn("varchar", new SFVarchar(100)));

    String schemaString =
        "{\"schema\":["
            + "{\"dataType\":{\"type\":\"date\"},\"name\":\"date\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"date\"},\"name\":\"date\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"date\"},\"name\":\"date\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"date\"},\"name\":\"date\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"date\"},\"name\":\"date\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"date\"},\"name\":\"date\",\"nullable\":false},"
            + "{\"dataType\":{\"type\":\"date\"},\"name\":\"date\",\"nullable\":false},"
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
