package org.apache.beam.sdk.io.snowflake.data.datetime;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

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
