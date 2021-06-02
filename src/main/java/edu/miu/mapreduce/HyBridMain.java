package edu.miu.mapreduce;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class HyBridMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new HyBridMain(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        FileUtils.deleteDirectory(new File(args[1]));
        //FileUtil.fullyDelete(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "PairApproach");
        job.setJarByClass(HyBridMain.class);

        job.setMapperClass(HyBridMapper.class);
        job.setReducerClass(HyBridReducer.class);
        job.setNumReduceTasks(2);
        job.setPartitionerClass(PairPartitioner.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }


}
