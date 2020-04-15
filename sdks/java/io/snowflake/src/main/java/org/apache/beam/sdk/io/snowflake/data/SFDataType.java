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
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonSubTypes;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonSubTypes.Type;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFDate;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFDateTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestamp;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestampLTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestampNTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestampTZ;
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

/** Interface for data types to provide SQLs for themselves. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @Type(value = SFDate.class, name = "date"),
  @Type(value = SFDateTime.class, name = "datetime"),
  @Type(value = SFTime.class, name = "time"),
  @Type(value = SFTimestamp.class, name = "timestamp"),
  @Type(value = SFTimestampLTZ.class, name = "timestamp_ltz"),
  @Type(value = SFTimestampNTZ.class, name = "timestamp_ntz"),
  @Type(value = SFTimestampTZ.class, name = "timestamp_tz"),
  @Type(value = SFBoolean.class, name = "boolean"),
  @Type(value = SFDecimal.class, name = "decimal"),
  @Type(value = SFDouble.class, name = "double"),
  @Type(value = SFFloat.class, name = "float"),
  @Type(value = SFInteger.class, name = "integer"),
  @Type(value = SFNumber.class, name = "number"),
  @Type(value = SFNumeric.class, name = "numeric"),
  @Type(value = SFReal.class, name = "real"),
  @Type(value = SFArray.class, name = "array"),
  @Type(value = SFObject.class, name = "object"),
  @Type(value = SFVariant.class, name = "variant"),
  @Type(value = SFBinary.class, name = "binary"),
  @Type(value = SFChar.class, name = "char"),
  @Type(value = SFString.class, name = "string"),
  @Type(value = SFText.class, name = "text"),
  @Type(value = SFVarBinary.class, name = "varbinary"),
  @Type(value = SFVarchar.class, name = "varchar"),
})
public interface SFDataType extends Serializable {
  String sql();
}
