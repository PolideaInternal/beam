package org.apache.beam.sdk.io.snowflake.data.numeric;

public class SFDecimal extends SFNumber {
  public SFDecimal(int precision, int scale) {
    super(precision, scale);
  }

  public static SFDecimal of(int precision, int scale) {
    return new SFDecimal(precision, scale);
  }
}
