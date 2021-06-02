package edu.miu.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairsReducer extends Reducer<Pair, IntWritable,Pair, DoubleWritable> {

    private int count;

    public PairsReducer(){
        count = 1;
    }

    @Override
    protected void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable value : values)
            sum += value.get();
        if(key.v.toString().equals("*"))
            count = sum;
        else
            context.write(key,new DoubleWritable(sum/count));
    }
}
