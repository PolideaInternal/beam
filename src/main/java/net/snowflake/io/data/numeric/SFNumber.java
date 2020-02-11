package net.snowflake.io.data.numeric;

import java.io.Serializable;
import net.snowflake.io.data.SFDataType;

public class SFNumber implements SFDataType, Serializable {
  private int precision = 38;
  private int scale = 0;

  public static SFNumber of() {
    return new SFNumber();
  }

  public static SFNumber of(int precision, int scale) {
    return new SFNumber(precision, scale);
  }

  public SFNumber() {}

  public SFNumber(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public String sql() {
    return String.format("NUMBER(%d,%d)", precision, scale);
  }
}
