package com.polidea.snowflake.io;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.ResultSet;
import java.util.Base64;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class ReadPipelineExample {

  static class Parse extends DoFn<KV<Integer, String>, String> implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().toString());
    }
  }

  public interface ExamplePipelineOptions extends SnowflakePipelineOptions {

    @Description("Table name to connect to.")
    String getTable();

    void setTable(String table);

    @Description("Destination of output data.")
    String getOutput();

    void setOutput(String output);
  }

  public static void main(String[] args) {
    ExamplePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ExamplePipelineOptions.class);
    Pipeline pipelineRead = Pipeline.create(options);

    String table = options.getTable();
    String output = options.getOutput();

    SnowflakeIO.DataSourceConfiguration dc = getDataSourceConfiguration(options);

    PCollection<KV<Integer, String>> namesAndIds =
        pipelineRead.apply(
            "Read from IO",
            SnowflakeIO.<KV<Integer, String>>read()
                .withDataSourceConfiguration(dc)
                .withQuery(String.format("SELECT id, name FROM %s LIMIT 1000;", table))
                .withRowMapper(
                    new SnowflakeIO.RowMapper<KV<Integer, String>>() {
                      public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
                        return KV.of(resultSet.getInt(1), resultSet.getString(2));
                      }
                    })
                .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

    PDone printableData =
        namesAndIds
            .apply("Find elements to write", ParDo.of(new Parse()))
            .apply("Write to text file", TextIO.write().to(output));

    PipelineResult pipelineResult = pipelineRead.run(options);
    pipelineResult.waitUntilFinish();
  }

  private static SnowflakeIO.DataSourceConfiguration getDataSourceConfiguration(
      ExamplePipelineOptions options) {
    if (options.getOauthToken() != null && !options.getOauthToken().isEmpty()) {
      return SnowflakeIO.DataSourceConfiguration.create()
          .withUrl(options.getUrl())
          .withServerName(options.getServerName())
          .withOauthToken(options.getOauthToken())
          .withDatabase(options.getDatabase())
          .withWarehouse(options.getWarehouse())
          .withSchema(options.getSchema());
    } else if (!options.getUsername().isEmpty() && !options.getPassword().isEmpty()) {
      return SnowflakeIO.DataSourceConfiguration.create()
          .withUrl(options.getUrl())
          .withServerName(options.getServerName())
          .withUsername(options.getUsername())
          .withPassword(options.getPassword())
          .withDatabase(options.getDatabase())
          .withSchema(options.getSchema());
    } else if (!options.getUsername().isEmpty()
        && !options.getPrivateKeyPath().isEmpty()
        && !options.getPrivateKeyPassphrase().isEmpty()) {
      return SnowflakeIO.DataSourceConfiguration.create()
          .withUrl(options.getUrl())
          .withServerName(options.getServerName())
          .withUsername(options.getUsername())
          .withPrivateKey(
              getPrivateKey(options.getPrivateKeyPath(), options.getPrivateKeyPassphrase()))
          .withDatabase(options.getDatabase())
          .withSchema(options.getSchema());
    }
    throw new RuntimeException("Can't create DataSourceConfiguration from options");
  }

  private static PrivateKey getPrivateKey(String privateKeyPath, String privateKeyPassphrase) {
    try {
      byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyPath));

      String encrypted = new String(keyBytes);
      encrypted = encrypted.replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "");
      encrypted = encrypted.replace("-----END ENCRYPTED PRIVATE KEY-----", "");
      EncryptedPrivateKeyInfo pkInfo =
          new EncryptedPrivateKeyInfo(Base64.getMimeDecoder().decode(encrypted));
      PBEKeySpec keySpec = new PBEKeySpec(privateKeyPassphrase.toCharArray());
      SecretKeyFactory pbeKeyFactory = SecretKeyFactory.getInstance(pkInfo.getAlgName());
      PKCS8EncodedKeySpec encodedKeySpec = pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));

      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePrivate(encodedKeySpec);
    } catch (Exception ex) {
      throw new RuntimeException("Can't create PrivateKey from options");
    }
  }
}
