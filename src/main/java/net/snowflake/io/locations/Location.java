package net.snowflake.io.locations;

public interface Location {
  /** Returns location of files where/to copy. It can be stage name or bucket path. */
  String getIntegration();

  String getFilesLocationForCopy();

  Boolean isUsingIntegration();

  Boolean isInternal();

  String getFilesPath();

  String getStage();
}
