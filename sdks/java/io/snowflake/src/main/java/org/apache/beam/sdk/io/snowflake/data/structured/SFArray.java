package org.apache.beam.sdk.io.snowflake.data.structured;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

public class SFArray implements SFDataType {
  public SFArray() {}

  public static SFArray of() {
    return new SFArray();
  }

  @Override
  public String sql() {
    return "ARRAY";
  }
}
