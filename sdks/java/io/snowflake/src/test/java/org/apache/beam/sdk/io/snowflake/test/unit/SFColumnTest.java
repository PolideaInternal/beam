package org.apache.beam.sdk.io.snowflake.test.unit;

import org.apache.beam.sdk.io.snowflake.data.SFColumn;
import org.apache.beam.sdk.io.snowflake.data.text.SFVarchar;
import org.junit.Test;

public class SFColumnTest {
  @Test
  public void testVarcharColumn() {
    SFColumn column = SFColumn.of("id", SFVarchar.of());

    assert column.sql().equals("id VARCHAR");
  }

  @Test
  public void testNullColumn() {
    SFColumn column = SFColumn.of("id", SFVarchar.of(), true);

    assert column.sql().equals("id VARCHAR NULL");
  }
}
