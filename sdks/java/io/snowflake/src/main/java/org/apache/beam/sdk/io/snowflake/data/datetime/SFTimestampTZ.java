package org.apache.beam.sdk.io.snowflake.data.datetime;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

public class SFTimestampTZ implements SFDataType {
  public SFTimestampTZ() {}

  public static SFTimestampTZ of() {
    return new SFTimestampTZ();
  }

  @Override
  public String sql() {
    return "TIMESTAMP_TZ";
  }
}
