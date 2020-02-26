package org.apache.beam.sdk.io.snowflake.test.unit;

import org.apache.beam.sdk.io.snowflake.data.structured.SFArray;
import org.apache.beam.sdk.io.snowflake.data.structured.SFObject;
import org.apache.beam.sdk.io.snowflake.data.structured.SFVariant;
import org.junit.Test;

public class SFStructuredDataTest {
  @Test
  public void testVariant() {
    SFVariant variant = SFVariant.of();

    assert variant.sql().equals("VARIANT");
  }

  @Test
  public void testArray() {
    SFArray array = SFArray.of();

    assert array.sql().equals("ARRAY");
  }

  @Test
  public void testObject() {
    SFObject object = SFObject.of();

    assert object.sql().equals("OBJECT");
  }
}
