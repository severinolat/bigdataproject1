package edu.miu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class HyBridMapper extends Mapper<LongWritable, Text,Pair, IntWritable> {

    HashMap<Pair,Integer> hashMap;

    public HyBridMapper() {
        hashMap = new HashMap<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] events = line.split(" ");

        for(int i = 0; i< events.length; i++){

            for(int k=i+1; k< events.length; k++){

                if(events[i].equals(events[k]))
                    break;
                Pair newPair = new Pair(events[i],events[k]);
                if(hashMap.containsKey(newPair)){
                    int oldCount = hashMap.get(newPair);
                    hashMap.put(newPair,oldCount+1);
                }
                else{
                    hashMap.put(newPair,1);
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        hashMap.forEach((key,value) -> {
            try {
                context.write(key,new IntWritable(value));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
