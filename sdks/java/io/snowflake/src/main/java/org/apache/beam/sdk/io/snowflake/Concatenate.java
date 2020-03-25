/*
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
package org.apache.beam.sdk.io.snowflake;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine;

/** Helper CombineFn for joining two lists of strings into one. */
public class Concatenate extends Combine.CombineFn<String, List<String>, List<String>> {
  @Override
  public List<String> createAccumulator() {
    return new ArrayList<>();
  }

  @Override
  public List<String> addInput(List<String> mutableAccumulator, String input) {
    mutableAccumulator.add(String.format("'%s'", input));
    return mutableAccumulator;
  }

  @Override
  public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
    List<String> result = createAccumulator();
    for (List<String> accumulator : accumulators) {
      result.addAll(accumulator);
    }
    return result;
  }

  @Override
  public List<String> extractOutput(List<String> accumulator) {
    return accumulator;
  }
}
