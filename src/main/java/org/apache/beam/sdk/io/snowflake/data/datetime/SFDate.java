package org.apache.beam.sdk.io.snowflake.data.datetime;

import org.apache.beam.sdk.io.snowflake.data.SFDataType;

public class SFDate implements SFDataType {
  public SFDate() {}

  public static SFDate of() {
    return new SFDate();
  }

  @Override
  public String sql() {
    return "DATE";
  }
}
