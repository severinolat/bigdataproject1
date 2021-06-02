package edu.miu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartitioner extends Partitioner<Pair, IntWritable> {

        @Override
        public int getPartition(Pair key, IntWritable value, int numberOfReducers) {

            return Integer.parseInt(key.k.toString()) % numberOfReducers;




        }

}
