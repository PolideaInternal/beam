package com.polidea.snowflake.io; /*
                                   * Licensed to the Apache Software Foundation (ASF) under one
                                   * or more contributor license agreements.  See the NOTICE file
                                   * distributed with this work for additional information
                                   * regarding copyright ownership.  The ASF licenses this file
                                   * to you under the Apache License, Version 2.0 (the
                                   * "License"); you may not use this file except in compliance
                                   * with the License.  You may obtain a copy of the License at
                                   *
                                   *     http://www.apache.org/licenses/LICENSE-2.0
                                   *
                                   * Unless required by applicable law or agreed to in writing, software
                                   * distributed under the License is distributed on an "AS IS" BASIS,
                                   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
                                   * See the License for the specific language governing permissions and
                                   * limitations under the License.
                                   */

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

@AutoValue
public abstract class TestRow implements Serializable, Comparable<TestRow> {
  /** Manually create a test row. */
  public static TestRow create(Integer id, String name) {
    return new AutoValue_TestRow(id, name);
  }

  public abstract Integer id();

  public abstract String name();

  @Override
  public int compareTo(TestRow other) {
    return id().compareTo(other.id());
  }

  public static TestRow fromSeed(Integer seed) {
    return create(seed, getNameForSeed(seed));
  }

  public static String getNameForSeed(Integer seed) {
    return "Testval" + seed;
  }

  public static class SelectNameFn extends DoFn<TestRow, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().name());
    }
  }

  public static class DeterministicallyConstructTestRowFn extends DoFn<Long, TestRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(fromSeed(c.element().intValue()));
    }
  }

  /**
   * Precalculated hashes - you can calculate an entry by running com.polidea.snowflake.io.HashingFn
   * on the name() for the rows generated from seeds in [0, n).
   */
  private static final ImmutableMap<Integer, String> EXPECTED_HASHES =
      ImmutableMap.of(1000, "c0f993a54cd47a51802853d80e98efa2");

  public static String getExpectedHashForRowCount(int rowCount)
      throws UnsupportedOperationException {
    return IOITHelper.getHashForRecordCount(rowCount, EXPECTED_HASHES);
  }
}
