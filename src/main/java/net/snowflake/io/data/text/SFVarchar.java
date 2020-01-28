package net.snowflake.io.data.text;

import java.io.Serializable;
import net.snowflake.io.data.SFDataType;

public class SFVarchar implements SFDataType, Serializable {
    private static final Long MAX_LENGTH = 16777216L;
    private Long length;

    public static SFVarchar of() {
        return new SFVarchar();
    }

    public static SFVarchar of(long length) {
        return new SFVarchar(length);
    }

    public SFVarchar() {
    }

    public SFVarchar(long length) {
        if (length > MAX_LENGTH) {
            throw new IllegalArgumentException();
        }
        this.length = length;
    }

    @Override
    public String sql() {
        if (length != null) {
            return String.format("VARCHAR(%d)", length);
        }
        return "VARCHAR";
    }
}
