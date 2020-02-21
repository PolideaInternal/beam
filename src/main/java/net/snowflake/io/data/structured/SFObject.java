package net.snowflake.io.data.structured;

import net.snowflake.io.data.SFDataType;

public class SFObject implements SFDataType {
  public SFObject() {}

  public static SFObject of() {
    return new SFObject();
  }

  @Override
  public String sql() {
    return "OBJECT";
  }
}
