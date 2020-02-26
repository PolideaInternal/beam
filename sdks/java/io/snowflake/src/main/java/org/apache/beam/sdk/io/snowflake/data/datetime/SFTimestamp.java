package org.apache.beam.sdk.io.snowflake.data.datetime;

public class SFTimestamp extends SFTimestampNTZ {
  public static SFTimestamp of() {
    return new SFTimestamp();
  }
}
