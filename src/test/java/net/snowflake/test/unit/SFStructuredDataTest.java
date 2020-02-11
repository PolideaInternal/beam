package net.snowflake.test.unit;

import net.snowflake.io.data.structured.SFArray;
import net.snowflake.io.data.structured.SFObject;
import net.snowflake.io.data.structured.SFVariant;
import org.junit.Test;

public class SFStructuredDataTest {
  @Test
  public void testVariant() {
    SFVariant variant = new SFVariant();

    assert variant.sql().equals("VARIANT");
  }

  @Test
  public void testArray() {
    SFArray array = new SFArray();

    assert array.sql().equals("ARRAY");
  }

  @Test
  public void testObject() {
    SFObject object = new SFObject();

    assert object.sql().equals("OBJECT");
  }
}
