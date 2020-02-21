package net.snowflake.io.data.datetime;

import net.snowflake.io.data.SFDataType;

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
