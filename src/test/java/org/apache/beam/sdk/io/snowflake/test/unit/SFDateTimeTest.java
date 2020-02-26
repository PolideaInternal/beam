package org.apache.beam.sdk.io.snowflake.test.unit;

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

    assert date.sql().equals("DATE");
  }

  @Test
  public void testDateTime() {
    SFDateTime dateTime = SFDateTime.of();

    assert dateTime.sql().equals("TIMESTAMP_NTZ");
  }

  @Test
  public void testTime() {
    SFTime time = SFTime.of();

    assert time.sql().equals("TIME");
  }

  @Test
  public void testTimestamp() {
    SFTimestamp timestamp = SFTimestamp.of();

    assert timestamp.sql().equals("TIMESTAMP_NTZ");
  }

  @Test
  public void testTimestampNTZ() {
    SFTimestampNTZ timestamp = SFTimestampNTZ.of();

    assert timestamp.sql().equals("TIMESTAMP_NTZ");
  }

  @Test
  public void testTimestampLTZ() {
    SFTimestampLTZ timestamp = SFTimestampLTZ.of();

    assert timestamp.sql().equals("TIMESTAMP_LTZ");
  }

  @Test
  public void testTimestampTZ() {
    SFTimestampTZ timestamp = SFTimestampTZ.of();

    assert timestamp.sql().equals("TIMESTAMP_TZ");
  }
}
