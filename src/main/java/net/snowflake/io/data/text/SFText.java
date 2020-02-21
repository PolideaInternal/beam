package net.snowflake.io.data.text;

public class SFText extends SFVarchar {
  public SFText() {}

  public SFText(long maxLength) {
    super(maxLength);
  }

  public static SFText of() {
    return new SFText();
  }

  public static SFText of(long maxLength) {
    return new SFText(maxLength);
  }
}
