package edu.miu.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Pair implements WritableComparable {
    public Text k;
    public Text v;

    public Pair() {
        this.k = new Text();
        this.v = new Text();
    }

    public Pair(Text k, Text v) {
        this.k = k;
        this.v = v;
    }

    public Pair(String k, String v) {
        this.k = new Text(k);
        this.v = new Text(v);
    }

    @Override
    public boolean equals(Object b) {
        Pair p = (Pair) b;
        return p.k.equals(this.k) && p.v.equals(this.v);
    }

    @Override
    public int hashCode() {
        return k.hashCode() * 10 + v.hashCode();
    }

    @Override
    public String toString() {
        return k + "," + v;
    }


    @Override
    public void readFields(DataInput arg0) throws IOException {

        this.k.readFields(arg0);
        this.v.readFields(arg0);
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        this.k.write(arg0);
        this.v.write(arg0);
    }

    @Override
    public int compareTo(Object o) {

        Pair p1 = (Pair) o;
        int k = this.k.compareTo(p1.k);

        if (k != 0)
            return k;
        else
            return this.v.compareTo(p1.v);
    }

    public Text getK() {
        return k;
    }

    public void setK(Text k) {
        this.k = k;
    }

    public Text getV() {
        return v;
    }

    public void setV(Text v) {
        this.v = v;
    }
}