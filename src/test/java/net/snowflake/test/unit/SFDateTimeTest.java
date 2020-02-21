package net.snowflake.test.unit;

import net.snowflake.io.data.datetime.SFDate;
import net.snowflake.io.data.datetime.SFDateTime;
import net.snowflake.io.data.datetime.SFTime;
import net.snowflake.io.data.datetime.SFTimestamp;
import net.snowflake.io.data.datetime.SFTimestampLTZ;
import net.snowflake.io.data.datetime.SFTimestampNTZ;
import net.snowflake.io.data.datetime.SFTimestampTZ;
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
