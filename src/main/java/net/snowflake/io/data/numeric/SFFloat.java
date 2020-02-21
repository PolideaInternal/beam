package net.snowflake.io.data.numeric;

import net.snowflake.io.data.SFDataType;

public class SFFloat implements SFDataType {
  public SFFloat() {}

  public static SFFloat of() {
    return new SFFloat();
  }

  @Override
  public String sql() {
    return "FLOAT";
  }
}
