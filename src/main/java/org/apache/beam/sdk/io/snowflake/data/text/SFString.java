package org.apache.beam.sdk.io.snowflake.data.text;

public class SFString extends SFVarchar {
  public SFString() {}

  public SFString(long maxLength) {
    super(maxLength);
  }

  public static SFString of() {
    return new SFString();
  }

  public static SFString of(long maxLength) {
    return new SFString(maxLength);
  }
}
