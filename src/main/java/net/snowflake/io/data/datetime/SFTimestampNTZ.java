package net.snowflake.io.data.datetime;

import net.snowflake.io.data.SFDataType;

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
