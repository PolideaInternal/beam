package org.apache.beam.sdk.io.snowflake.data.logical;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

public class SFBoolean implements SFDataType {
  public SFBoolean() {}

  public static SFBoolean of() {
    return new SFBoolean();
  }

  @Override
  public String sql() {
    return "BOOLEAN";
  }
}
