package net.snowflake.io.data.datetime;

import net.snowflake.io.data.SFDataType;

public class SFTime implements SFDataType {
  public SFTime() {}

  public static SFTime of() {
    return new SFTime();
  }

  @Override
  public String sql() {
    return "TIME";
  }
}
