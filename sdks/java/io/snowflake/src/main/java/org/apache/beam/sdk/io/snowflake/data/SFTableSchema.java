package org.apache.beam.sdk.io.snowflake.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;

public class SFTableSchema implements Serializable {
  private SFColumn[] columns;

  public static SFTableSchema of(SFColumn... columns) {
    return new SFTableSchema(columns);
  }

  public SFTableSchema(SFColumn... columns) {
    this.columns = columns;
  }

  public String sql() {
    List<String> columnsSqls = new ArrayList<>();
    for (SFColumn column : columns) {
      columnsSqls.add(column.sql());
    }

    return Joiner.on(", ").join(columnsSqls);
  }
}
