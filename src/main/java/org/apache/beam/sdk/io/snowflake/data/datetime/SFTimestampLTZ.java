package org.apache.beam.sdk.io.snowflake.data.datetime;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

public class SFTimestampLTZ implements SFDataType {
  public SFTimestampLTZ() {}

  public static SFTimestampLTZ of() {
    return new SFTimestampLTZ();
  }

  @Override
  public String sql() {
    return "TIMESTAMP_LTZ";
  }
}
