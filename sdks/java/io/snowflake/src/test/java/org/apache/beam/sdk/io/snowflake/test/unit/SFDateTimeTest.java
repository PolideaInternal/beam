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

import org.apache.beam.sdk.io.snowflake.data.datetime.SFDate;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFDateTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestamp;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestampLTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestampNTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestampTZ;
import org.junit.Test;

public class SFDateTimeTest {
  @Test
  public void testDate() {
    SFDate date = SFDate.of();

    assertEquals("DATE", date.sql());
  }

  @Test
  public void testDateTime() {
    SFDateTime dateTime = SFDateTime.of();

    assertEquals("TIMESTAMP_NTZ", dateTime.sql());
  }

  @Test
  public void testTime() {
    SFTime time = SFTime.of();

    assertEquals("TIME", time.sql());
  }

  @Test
  public void testTimestamp() {
    SFTimestamp timestamp = SFTimestamp.of();

    assertEquals("TIMESTAMP_NTZ", timestamp.sql());
  }

  @Test
  public void testTimestampNTZ() {
    SFTimestampNTZ timestamp = SFTimestampNTZ.of();

    assertEquals("TIMESTAMP_NTZ", timestamp.sql());
  }

  @Test
  public void testTimestampLTZ() {
    SFTimestampLTZ timestamp = SFTimestampLTZ.of();

    assertEquals("TIMESTAMP_LTZ", timestamp.sql());
  }

  @Test
  public void testTimestampTZ() {
    SFTimestampTZ timestamp = SFTimestampTZ.of();

    assertEquals("TIMESTAMP_TZ", timestamp.sql());
  }
}
