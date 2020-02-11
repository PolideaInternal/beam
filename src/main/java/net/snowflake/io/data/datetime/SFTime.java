package net.snowflake.io.data.datetime;

import net.snowflake.io.data.SFDataType;

public class SFTime implements SFDataType {
  public SFTime(){}

  @Override
  public String sql() {
    return "TIME";
  }
}
