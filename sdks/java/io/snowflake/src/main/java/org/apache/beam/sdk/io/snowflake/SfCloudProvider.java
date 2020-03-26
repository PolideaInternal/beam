package org.apache.beam.sdk.io.snowflake;

public interface SfCloudProvider {

    String getStoragePath(String fileName);

    void put();

    void remove(String bucketPath, String bucketName);

}
