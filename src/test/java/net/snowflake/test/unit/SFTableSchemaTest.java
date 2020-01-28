package net.snowflake.test.unit;

import net.snowflake.io.data.SFColumn;
import net.snowflake.io.data.SFTableSchema;
import net.snowflake.io.data.numeric.SFDouble;
import net.snowflake.io.data.text.SFVarchar;
import org.junit.Test;

public class SFTableSchemaTest {
    @Test
    public void testOneColumn() {
        SFTableSchema schema = new SFTableSchema(SFColumn.of("id", SFVarchar.of()));

        assert schema.sql().equals("id VARCHAR");
    }

    @Test
    public void testTwoColumns() {
        SFTableSchema schema = new SFTableSchema(
                SFColumn.of("id", new SFVarchar()),
                SFColumn.of("tax", new SFDouble())
        );

        assert schema.sql().equals("id VARCHAR, tax FLOAT");
    }

}
