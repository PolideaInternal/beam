package net.snowflake.io.data.logical;

import net.snowflake.io.data.SFDataType;

public class SFBoolean implements SFDataType {
    public SFBoolean() {
    }

    @Override
    public String sql() {
        return "BOOLEAN";
    }

}
