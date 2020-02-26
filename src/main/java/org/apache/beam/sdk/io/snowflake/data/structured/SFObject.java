package org.apache.beam.sdk.io.snowflake.data.structured;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

public class SFObject implements SFDataType {
  public SFObject() {}

  public static SFObject of() {
    return new SFObject();
  }

  @Override
  public String sql() {
    return "OBJECT";
  }
}
