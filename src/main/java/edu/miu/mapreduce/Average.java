package edu.miu.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;

public class Average extends Configured implements Tool{

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {


        private Text ipAddress = new Text();
        String QUANTITY_PATTERN = "[0-9]+$";

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            String[] arrOfStr = value.toString().split("\\s+");
            ipAddress.set(arrOfStr[0]);
            IntWritable quantity = new IntWritable(Integer.valueOf(getQuantity(value.toString())));

                context.write(ipAddress, quantity);
        }

        String getQuantity(String value){
            Pattern pattern = Pattern.compile(QUANTITY_PATTERN);
            Matcher matcher = pattern.matcher(value);
            if (matcher.find()) {
                return matcher.group();
            } else{
                return "1";
            }
        }

    }

    public static class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
                count ++;
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new Average(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {


        FileUtils.deleteDirectory(new File(args[1]));
        //FileUtil.fullyDelete(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "Average");
        job.setJarByClass(Average.class);

       job.setMapperClass(Average.AverageMapper.class);
        job.setReducerClass(Average.AverageReducer.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
