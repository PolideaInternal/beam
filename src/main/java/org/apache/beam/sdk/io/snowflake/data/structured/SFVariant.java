package org.apache.beam.sdk.io.snowflake.data.structured;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

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
