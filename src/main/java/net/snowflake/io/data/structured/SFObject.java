package net.snowflake.io.data.structured;

import net.snowflake.io.data.SFDataType;

public class SFObject implements SFDataType {
  public SFObject() {}

  @Override
  public String sql() {
    return "OBJECT";
  }
}
