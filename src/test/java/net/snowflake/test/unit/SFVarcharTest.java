package net.snowflake.test.unit;

import net.snowflake.io.data.text.SFBinary;
import net.snowflake.io.data.text.SFChar;
import net.snowflake.io.data.text.SFString;
import net.snowflake.io.data.text.SFText;
import net.snowflake.io.data.text.SFVarBinary;
import net.snowflake.io.data.text.SFVarchar;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SFVarcharTest {
  @Test
  public void testSingleVarchar() {
    SFVarchar varchar = SFVarchar.of();

    assert varchar.sql().equals("VARCHAR");
  }

  @Test
  public void testSingleVarcharWithLimit() {
    SFVarchar varchar = SFVarchar.of(100);

    assert varchar.sql().equals("VARCHAR(100)");
  }

  @Test
  public void testString() {
    SFString str = SFString.of();

    assert str.sql().equals("VARCHAR");
  }

  @Test
  public void testText() {
    SFText text = SFText.of();

    assert text.sql().equals("VARCHAR");
  }

  @Test
  public void testBinary() {
    SFBinary binary = SFBinary.of();

    assert binary.sql().equals("BINARY");
  }

  @Test
  public void testVarBinary() {
    SFVarBinary binary = SFVarBinary.of();

    assert binary.sql().equals("BINARY");
  }

  @Test
  public void testBinaryWithLimit() {
    SFBinary binary = SFBinary.of(100);

    assert binary.sql().equals("BINARY(100)");
  }

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testBinaryReachesLimit() {
    exceptionRule.expect(IllegalArgumentException.class);
    SFBinary.of(8388609L);
  }

  @Test
  public void testChar() {
    SFChar sfChar = SFChar.of();

    assert sfChar.sql().equals("VARCHAR(1)");
  }
}
