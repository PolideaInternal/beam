package net.snowflake.io.data.text;

public class SFChar extends SFVarchar {
  public SFChar() {
    super(1);
  }

  public static SFChar of() {
    return new SFChar();
  }
}
