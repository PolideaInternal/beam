package org.apache.beam.sdk.io.snowflake.data.numeric;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

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
