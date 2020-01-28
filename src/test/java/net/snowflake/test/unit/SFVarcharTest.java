package net.snowflake.test.unit;

import net.snowflake.io.data.text.SFVarchar;
import org.junit.Test;

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
}
