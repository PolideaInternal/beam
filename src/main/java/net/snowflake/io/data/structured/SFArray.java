package net.snowflake.io.data.structured;

import net.snowflake.io.data.SFDataType;

public class SFArray implements SFDataType {
  public SFArray() {}

  @Override
  public String sql() {
    return "ARRAY";
  }
}
