package org.apache.beam.sdk.io.snowflake;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GCSProvider implements SfCloudProvider {


    @Override
    public String getStoragePath(String fileName) {
        return null;
    }

    @Override
    public void put() {

    }

    @Override
    public void remove(String bucketPath, String bucketName) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(bucketPath));
        for (Blob blob : blobs.iterateAll()) {
            storage.delete(blob.getBlobId());
        }
    }


}
