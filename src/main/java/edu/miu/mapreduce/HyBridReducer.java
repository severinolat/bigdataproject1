package edu.miu.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

public class HyBridReducer extends Reducer<Pair, IntWritable, Text, MyMapWritable> {

    private HashMap<String, Integer> sumMap;
    private String currentItem;
    private double total;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        sumMap = new HashMap<>();
        total = 0;
    }

    @Override
    protected void reduce(Pair pair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if(currentItem == null){
            currentItem = pair.getK().toString();
        }
        else if(!currentItem.equals((pair.getK().toString()))){
            MyMapWritable finalMap = new MyMapWritable();
            sumMap.forEach((key,value) -> {
                double r = value / total;
                DecimalFormat df = new DecimalFormat("#.###");
                finalMap.put(new Text(key), new DoubleWritable(Double.parseDouble(df.format(r))));
            });
            context.write(new Text(currentItem), finalMap);

            currentItem = pair.getK().toString();
            sumMap = new HashMap<>();
            total = 0;
        }

        int currentTotal = 0;
        for(IntWritable intWritable : values){
            currentTotal += intWritable.get();
        }
        total += currentTotal;
        sumMap.put(pair.getV().toString(), currentTotal);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        MyMapWritable finalMap = new MyMapWritable();
        sumMap.forEach((key,value) -> {
            double r = value / total;
            DecimalFormat df = new DecimalFormat("#.###");
            finalMap.put(new Text(key), new DoubleWritable(Double.parseDouble(df.format(r))));
        });
        context.write(new Text(currentItem), finalMap);
    }
}
