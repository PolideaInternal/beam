package org.apache.beam.sdk.io.snowflake.data.datetime;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

public class SFTimestampNTZ implements SFDataType {
  public SFTimestampNTZ() {}

  public static SFTimestampNTZ of() {
    return new SFTimestampNTZ();
  }

  @Override
  public String sql() {
    return "TIMESTAMP_NTZ";
  }
}
