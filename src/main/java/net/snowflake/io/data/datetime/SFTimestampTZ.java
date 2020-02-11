package net.snowflake.io.data.datetime;

import net.snowflake.io.data.SFDataType;

public class SFTimestampTZ implements SFDataType {
  public SFTimestampTZ(){}

  @Override
  public String sql() {
    return "TIMESTAMP_TZ";
  }
}
