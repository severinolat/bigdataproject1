package edu.miu.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

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

public class WordCountInMapper extends Configured implements Tool{

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {


        private Text word = new Text();
        private HashMap<String,Integer> hashMap = new HashMap<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            for (String token : value.toString().split("\\s+"))
            {
                int count = 1;

                if(hashMap.containsKey(token)) {
                    count += hashMap.get(token);
                }
                hashMap.put(token,count);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            hashMap.forEach((key,value) ->
            {
                IntWritable count = new IntWritable(value);
                try {
                    word.set(key);
                    context.write(word, count);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            );


        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new WordCountInMapper(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        FileUtils.deleteDirectory(new File(args[1]));
        //FileUtil.fullyDelete(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "WordCount");
        job.setJarByClass(WordCountInMapper.class);

        job.setMapperClass(WordCountInMapper.WordCountMapper.class);
        job.setReducerClass(WordCountInMapper.WordCountReducer.class);
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
