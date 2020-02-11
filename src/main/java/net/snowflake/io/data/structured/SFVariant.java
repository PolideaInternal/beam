package net.snowflake.io.data.structured;

import net.snowflake.io.data.SFDataType;

public class SFVariant implements SFDataType {
  public SFVariant() {}

  @Override
  public String sql() {
    return "VARIANT";
  }
}
