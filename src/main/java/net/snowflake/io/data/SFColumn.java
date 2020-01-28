package net.snowflake.io.data;

import java.io.Serializable;

public class SFColumn implements Serializable {
    private SFDataType dataType;
    private String name;
    private boolean _null;

    public static SFColumn of(String name, SFDataType dataType){
        return new SFColumn(name, dataType);
    }

    public SFColumn(String name, SFDataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public SFColumn(String name, SFDataType dataType, boolean _null) {
        this.dataType = dataType;
        this.name = name;
        this._null = _null;
    }

    public String sql(){
        String sql = String.format("%s %s", name, dataType.sql());
        if (_null){
            sql += " NULL";
        }
        return sql;
    }

}
