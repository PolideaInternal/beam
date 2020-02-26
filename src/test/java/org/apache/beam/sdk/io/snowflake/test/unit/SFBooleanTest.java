package org.apache.beam.sdk.io.snowflake.test.unit;

import org.apache.beam.sdk.io.snowflake.data.logical.SFBoolean;
import org.junit.Test;

public class SFBooleanTest {
  @Test
  public void testBoolean() {
    SFBoolean sfBoolean = SFBoolean.of();

    assert sfBoolean.sql().equals("BOOLEAN");
  }
}
