package net.snowflake.io.data.datetime;

import net.snowflake.io.data.SFDataType;

public class SFTimestampLTZ implements SFDataType {
  public SFTimestampLTZ() {}

  @Override
  public String sql() {
    return "TIMESTAMP_LTZ";
  }
}
