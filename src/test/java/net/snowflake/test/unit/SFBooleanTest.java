package net.snowflake.test.unit;

import net.snowflake.io.data.logical.SFBoolean;
import org.junit.Test;

public class SFBooleanTest {
  @Test
  public void testBoolean() {
    SFBoolean sfBoolean = SFBoolean.of();

    assert sfBoolean.sql().equals("BOOLEAN");
  }
}
