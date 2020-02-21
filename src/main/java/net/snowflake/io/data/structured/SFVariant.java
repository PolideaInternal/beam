package net.snowflake.io.data.structured;

import net.snowflake.io.data.SFDataType;

public class SFVariant implements SFDataType {
  public SFVariant() {}

  public static SFVariant of() {
    return new SFVariant();
  }

  @Override
  public String sql() {
    return "VARIANT";
  }
}
