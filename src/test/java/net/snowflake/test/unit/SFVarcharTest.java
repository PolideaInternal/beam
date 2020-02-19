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
    SFString str = new SFString();

    assert str.sql().equals("VARCHAR");
  }

  @Test
  public void testText() {
    SFText text = new SFText();

    assert text.sql().equals("VARCHAR");
  }

  @Test
  public void testBinary() {
    SFBinary binary = new SFBinary();

    assert binary.sql().equals("BINARY");
  }

  @Test
  public void testVarBinary() {
    SFVarBinary binary = new SFVarBinary();

    assert binary.sql().equals("BINARY");
  }

  @Test
  public void testBinaryWithLimit() {
    SFBinary binary = new SFBinary(100);

    assert binary.sql().equals("BINARY(100)");
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();
  @Test
  public void testBinaryReachesLimit() {
    exceptionRule.expect(IllegalArgumentException.class);
    SFBinary binary = new SFBinary(8388609L);
  }

  @Test
  public void testChar() {
    SFChar chartext = new SFChar();

    assert chartext.sql().equals("VARCHAR(1)");
  }
}
