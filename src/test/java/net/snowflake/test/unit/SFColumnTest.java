package net.snowflake.test.unit;

import net.snowflake.io.data.SFColumn;
import net.snowflake.io.data.text.SFVarchar;
import org.junit.Test;

public class SFColumnTest {
    @Test
    public void testVarcharColumn() {
        SFColumn column = SFColumn.of("id", SFVarchar.of());

        assert column.sql().equals("id VARCHAR");
    }

}
