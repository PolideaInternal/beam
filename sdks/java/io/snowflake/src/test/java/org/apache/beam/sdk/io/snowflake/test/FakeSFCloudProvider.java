package org.apache.beam.sdk.io.snowflake.test;

import org.apache.beam.sdk.io.snowflake.SfCloudProvider;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class FakeSFCloudProvider implements SfCloudProvider, Serializable {
    @Override
    public String getStoragePath(String fileName) {
        return null;
    }

    @Override
    public void put() {

    }

    @Override
    public void remove(String bucketPath, String bucketName) {
        Path path = Paths.get(String.format("./%s", bucketPath));
        try (Stream<Path> stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            throw new RuntimeException("Failed to remove files", e);
        }
    }
}
