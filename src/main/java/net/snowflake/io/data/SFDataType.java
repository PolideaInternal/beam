package net.snowflake.io.data;

import java.io.Serializable;

public interface SFDataType extends Serializable {
    String sql();
}
