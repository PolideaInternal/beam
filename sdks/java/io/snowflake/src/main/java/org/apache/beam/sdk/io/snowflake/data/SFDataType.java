package org.apache.beam.sdk.io.snowflake.data;

import java.io.Serializable;

public interface SFDataType extends Serializable {
  String sql();
}
