package net.snowflake.io.data.text;

import net.snowflake.io.data.SFDataType;

public class SFBinary implements SFDataType {

  private static final Long MAX_SIZE = 8388608L;

  private Long size; // bytes

  public SFBinary() {}

  public static SFBinary of() {
    return new SFBinary();
  }

  public static SFBinary of(long size) {
    return new SFBinary(size);
  }

  public SFBinary(long size) {
    if (size > MAX_SIZE) {
      throw new IllegalArgumentException();
    }
    this.size = size;
  }

  @Override
  public String sql() {
    if (size != null) {
      return String.format("BINARY(%d)", size);
    }
    return "BINARY";
  }
}
