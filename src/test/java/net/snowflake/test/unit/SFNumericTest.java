package net.snowflake.test.unit;

import net.snowflake.io.data.numeric.SFDecimal;
import net.snowflake.io.data.numeric.SFDouble;
import net.snowflake.io.data.numeric.SFFloat;
import net.snowflake.io.data.numeric.SFInteger;
import net.snowflake.io.data.numeric.SFNumber;
import net.snowflake.io.data.numeric.SFNumeric;
import net.snowflake.io.data.numeric.SFReal;
import org.junit.Test;

public class SFNumericTest {
  @Test
  public void testDecimal() {
    SFDecimal decimal = SFDecimal.of(20, 1);

    assert decimal.sql().equals("NUMBER(20,1)");
  }

  @Test
  public void testDouble() {
    SFDouble sfDouble = SFDouble.of();

    assert sfDouble.sql().equals("FLOAT");
  }

  @Test
  public void testFloat() {
    SFFloat sfFloat = SFFloat.of();

    assert sfFloat.sql().equals("FLOAT");
  }

  @Test
  public void testInteger() {
    SFInteger sfInteger = SFInteger.of();

    assert sfInteger.sql().equals("NUMBER(38,0)");
  }

  @Test
  public void testNumber() {
    SFNumber sfNumber = SFNumber.of();

    assert sfNumber.sql().equals("NUMBER(38,0)");
  }

  @Test
  public void testNumeric() {
    SFNumeric sfNumeric = SFNumeric.of(33, 2);

    assert sfNumeric.sql().equals("NUMBER(33,2)");
  }

  @Test
  public void testReal() {
    SFReal sfReal = SFReal.of();

    assert sfReal.sql().equals("FLOAT");
  }
}
