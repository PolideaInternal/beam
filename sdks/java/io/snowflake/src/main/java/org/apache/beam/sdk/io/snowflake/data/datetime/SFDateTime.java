package org.apache.beam.sdk.io.snowflake.data.datetime;

public class SFDateTime extends SFTimestampNTZ {
  public static SFDateTime of() {
    return new SFDateTime();
  }
}
