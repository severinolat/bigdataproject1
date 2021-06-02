package edu.miu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.sql.sources.In;

import java.io.IOException;
import java.util.HashMap;

public class PairsMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {

    HashMap<Pair, Integer> hashMap;

    public PairsMapper(){

        hashMap = new HashMap<Pair,Integer>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] events = line.split(" ");

        for(int i = 0; i< events.length; i++){

            for(int k=i+1; k< events.length; k++){
                Pair newPair = new Pair(events[i],events[k]);
                Pair starPair = new Pair(events[i],"*");
                updateMap(newPair);
                updateMap(starPair);
            }
        }
    }

    private void updateMap(Pair pair){
        if(hashMap.containsKey(pair)){
            int oldCount = hashMap.get(pair);
            hashMap.put(pair,oldCount+1);
        }
        else{
            hashMap.put(pair,1);
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
