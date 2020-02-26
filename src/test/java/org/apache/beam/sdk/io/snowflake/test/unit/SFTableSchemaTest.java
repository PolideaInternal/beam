package org.apache.beam.sdk.io.snowflake.test.unit;

import org.apache.beam.sdk.io.snowflake.data.SFColumn;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.data.numeric.SFDouble;
import org.apache.beam.sdk.io.snowflake.data.text.SFVarchar;
import org.junit.Test;

public class SFTableSchemaTest {
  @Test
  public void testOneColumn() {
    SFTableSchema schema = new SFTableSchema(SFColumn.of("id", SFVarchar.of()));

    assert schema.sql().equals("id VARCHAR");
  }

  @Test
  public void testTwoColumns() {
    SFTableSchema schema =
        new SFTableSchema(SFColumn.of("id", new SFVarchar()), SFColumn.of("tax", new SFDouble()));

    assert schema.sql().equals("id VARCHAR, tax FLOAT");
  }
}
