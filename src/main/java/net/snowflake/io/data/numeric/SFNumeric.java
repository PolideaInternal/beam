package net.snowflake.io.data.numeric;

public class SFNumeric extends SFNumber {
  public SFNumeric(int precision, int scale) {
    super(precision, scale);
  }

  public static SFNumeric of(int precision, int scale) {
    return new SFNumeric(precision, scale);
  }
}
