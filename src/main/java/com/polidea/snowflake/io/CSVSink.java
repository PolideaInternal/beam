package com.polidea.snowflake.io;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;

public class CSVSink implements FileIO.Sink<String> {
  private String header;
  private transient PrintWriter writer;

  public CSVSink(List<String> colNames) {
    if (!colNames.isEmpty()) {
      this.header = Joiner.on(",").join(colNames);
    }
  }

  public CSVSink() {
    this.header = null;
  }

  @Override
  public void open(WritableByteChannel channel) throws IOException {
    writer = new PrintWriter(Channels.newOutputStream(channel));
    if (this.header != null) {
      writer.println(header);
    }
  }

  @Override
  public void write(String element) throws IOException {
    writer.println(Joiner.on(",").join(Collections.singleton(element)));
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }
}
