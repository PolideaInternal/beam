package net.snowflake.io.data;

import java.io.Serializable;

public class SFColumn implements Serializable {
  private SFDataType dataType;
  private String name;
  private boolean isNull;

  public static SFColumn of(String name, SFDataType dataType) {
    return new SFColumn(name, dataType);
  }

  public SFColumn(String name, SFDataType dataType) {
    this.name = name;
    this.dataType = dataType;
  }

  public SFColumn(String name, SFDataType dataType, boolean isNull) {
    this.dataType = dataType;
    this.name = name;
    this.isNull = isNull;
  }

  public String sql() {
    String sql = String.format("%s %s", name, dataType.sql());
    if (isNull) {
      sql += " NULL";
    }
    return sql;
  }
}
