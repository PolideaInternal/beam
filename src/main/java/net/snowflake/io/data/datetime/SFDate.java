package net.snowflake.io.data.datetime;

import net.snowflake.io.data.SFDataType;

public class SFDate implements SFDataType {
  public SFDate() {}

  @Override
  public String sql() {
    return "DATE";
  }
}
