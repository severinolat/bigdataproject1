package edu.miu.mapreduce;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Stripes extends Configured implements Tool
{

    private static MapWritable strip = new MapWritable();
    private static MapWritable sumofAllStrips = new MapWritable();
    private static Text keyTerm = new Text();


    public static class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {



        @Override
        public  void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            String[] allTerms = value.toString().split("\\s+");
            for (int i = 0; i<allTerms.length; i++){
                keyTerm.set(allTerms[i]);
                strip.clear();

                for (int j = i+1; j< allTerms.length; j++){
                    if (keyTerm.toString().equals(allTerms[j]))
                        break;
                    Text neighboardTerm = new Text(allTerms[j]);
                    if (strip.containsKey(neighboardTerm)) {
                        IntWritable count = (IntWritable) strip.get(neighboardTerm);
                        count.set(count.get() + 1);
                    } else {
                        strip.put(neighboardTerm, new IntWritable(1));
                    }
                }

                //write the strip to the content if map is not empty
                if (!strip.isEmpty()){
                    context.write(new Text(keyTerm),strip);
                }

            }


        }


    }


    private static void addStripToResultantStrip(MapWritable mapWritable){
        Set<Writable> allKeys =mapWritable.keySet();
        for (Writable key : allKeys){
            IntWritable keyCountInCurrentStrip = (IntWritable)mapWritable.get(key);
            if (sumofAllStrips.containsKey(key)){
                IntWritable keyCountInResultantStrip = (IntWritable) sumofAllStrips.get(key);
                keyCountInResultantStrip.set(keyCountInResultantStrip.get() + keyCountInCurrentStrip.get());
            } else {
                sumofAllStrips.put(key,keyCountInCurrentStrip);
            }
        }
    }





    public static   class StripesReducer extends Reducer<Text, MapWritable, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
        {
            sumofAllStrips.clear();

            // sum all strips into single strip
            for (MapWritable value : values){
                addStripToResultantStrip(value);
            }
            int totalCount = 0;
            //get the total count which is needed to calculate relative frequencies

            for(Map.Entry<Writable, Writable> entry : sumofAllStrips.entrySet()){
                totalCount += Integer.parseInt(entry.getValue().toString());
            }

            DecimalFormat decimalFormat = new DecimalFormat("0.00");
            StringBuilder stringBuilder = new StringBuilder();
            for (Map.Entry<Writable, Writable> entry : sumofAllStrips.entrySet()){
                if (stringBuilder.length() >0) stringBuilder.append(",");
                stringBuilder.append("(").append(entry.getKey().toString())
                        .append(",")
                        .append(decimalFormat.format(Integer.parseInt(entry.getValue().toString())/(double) totalCount))
                        .append(")");
            }
            context.write(key, new Text("[" + stringBuilder.toString() + "]"));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new Stripes(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {


        FileUtils.deleteDirectory(new File(args[1]));
        //FileUtil.fullyDelete(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "Stripes");
        job.setJarByClass(Stripes.class);

        job.setMapperClass(StripesMapper.class);
        job.setReducerClass(StripesReducer.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}




