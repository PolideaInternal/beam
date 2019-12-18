package com.polidea.snowflake.test;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.testing.TestPipelineOptions;

public interface SnowflakeTestPipelineOptions extends TestPipelineOptions, DataflowPipelineOptions {
  String getAccount();

  void setAccount(String account);

  String getUsername();

  void setUsername(String username);

  String getPassword();

  void setPassword(String password);

  String getTable();

  void setTable(String table);

  String getDatabase();

  void setDatabase(String database);

  String getSchema();

  void setSchema(String schema);

  String getSnowflakeRegion();

  void setSnowflakeRegion(String snowflakeRegion);

  String getOutput();

  void setOutput(String output);

  String getOauthToken();

  void setOauthToken(String oauthToken);

  String getPrivateKeyPath();

  void setPrivateKeyPath(String privateKeyPath);

  String getPrivateKeyPassphrase();

  void setPrivateKeyPassphrase(String keyPassphrase);

  String getTempRoot();

  void setTempRoot(String tempRoot);
}
