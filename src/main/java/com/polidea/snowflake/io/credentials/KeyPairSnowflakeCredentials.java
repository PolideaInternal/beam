package com.polidea.snowflake.io.credentials;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class KeyPairSnowflakeCredentials implements SnowflakeCredentials {
  private String username;
  private PrivateKey privateKey;

  public KeyPairSnowflakeCredentials(
      String username, String privateKeyPath, String privateKeyPassword) {
    this.username = username;
    this.privateKey = getPrivateKey(privateKeyPath, privateKeyPassword);
  }

  public KeyPairSnowflakeCredentials(String username, PrivateKey privateKey) {
    this.username = username;
    this.privateKey = privateKey;
  }

  private PrivateKey getPrivateKey(String privateKeyPath, String privateKeyPassphrase) {
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

  public String getUsername() {
    return username;
  }

  public PrivateKey getPrivateKey() {
    return privateKey;
  }
}
