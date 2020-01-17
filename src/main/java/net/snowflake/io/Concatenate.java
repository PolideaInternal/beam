package net.snowflake.io;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine;

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
