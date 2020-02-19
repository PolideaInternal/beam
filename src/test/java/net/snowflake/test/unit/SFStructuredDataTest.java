package net.snowflake.test.unit;

import net.snowflake.io.data.structured.SFArray;
import net.snowflake.io.data.structured.SFObject;
import net.snowflake.io.data.structured.SFVariant;
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
