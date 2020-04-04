package org.apache.beam.sdk.io.snowflake;

import java.util.ArrayList;
import java.util.List;

public class RunStatementResult<T> {
    private List<T> values;

    public RunStatementResult() {
        values = new ArrayList<>();
    }

    public boolean addAll(List<T> values) {
        return this.values.addAll(values);
    }

    public boolean add(T value) {
        return this.values.add(value);
    }

    public List<T> getAll(){
        return this.values;
    }
}
