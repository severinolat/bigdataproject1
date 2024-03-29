package edu.miu.mapreduce;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class MyMapWritable extends MapWritable {
    public MyMapWritable() {
        super();
    }

    @Override
    public String toString() {
        String out = "[";
        for (Entry<Writable, Writable> entry : this.entrySet()) {
            out += String.format("(%s, %s), ", entry.getKey().toString(), entry
                    .getValue().toString());
        }
        return out + "]";
    }
}
