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
    SFDate date = new SFDate();

    assert date.sql().equals("DATE");
  }

  @Test
  public void testDateTime() {
    SFDateTime dateTime = new SFDateTime();

    assert dateTime.sql().equals("TIMESTAMP_NTZ");
  }

  @Test
  public void testTime() {
    SFTime time = new SFTime();

    assert time.sql().equals("TIME");
  }

  @Test
  public void testTimestamp() {
    SFTimestamp timestamp = new SFTimestamp();

    assert timestamp.sql().equals("TIMESTAMP_NTZ");
  }

  @Test
  public void testTimestampNTZ() {
    SFTimestampNTZ timestamp = new SFTimestampNTZ();

    assert timestamp.sql().equals("TIMESTAMP_NTZ");
  }

  @Test
  public void testTimestampLTZ() {
    SFTimestampLTZ timestamp = new SFTimestampLTZ();

    assert timestamp.sql().equals("TIMESTAMP_LTZ");
  }

  @Test
  public void testTimestampTZ() {
    SFTimestampTZ timestamp = new SFTimestampTZ();

    assert timestamp.sql().equals("TIMESTAMP_TZ");
  }

}
