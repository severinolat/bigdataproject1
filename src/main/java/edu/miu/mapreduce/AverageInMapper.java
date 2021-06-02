package edu.miu.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.Writable;
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

public class AverageInMapper extends Configured implements Tool{

    public static class MyPair implements Writable {

        private  int sum;
        private int count;

        public MyPair(int sum, int count) {
            this.sum = sum;
            this.count = count;
        }

        public MyPair(){}

        @Override
        public void readFields(DataInput in) throws IOException {
            sum = in.readInt();
            count = in.readInt();

        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(sum);
            out.writeInt(count);

        }

        public static MyPair read(DataInput in) throws IOException{

            MyPair myPair = new MyPair();
            myPair.readFields(in);
            return myPair;
        }

        public int getSum() {
            return sum;
        }

        public void setSum(int sum) {
            this.sum = sum;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }




    }

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, MyPair>
    {

        private Text ipAddress = new Text();
        String QUANTITY_PATTERN = "[0-9]+$";
        HashMap<String,MyPair> hashMap = new HashMap<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            String[] arrOfStr = value.toString().split("\\s+");

            IntWritable quantity = new IntWritable(Integer.valueOf(getQuantity(value.toString())));

            int currentQuantity = Integer.valueOf(getQuantity(value.toString()));
            MyPair myPair = new MyPair(currentQuantity,1);
            if(hashMap.containsKey(arrOfStr[0])) {
                int preQuantity = hashMap.get(arrOfStr[0]).getSum();
                int preCount = hashMap.get(arrOfStr[0]).getCount();
                myPair = new MyPair(currentQuantity  + preQuantity, preCount +1);
            }
            hashMap.put(arrOfStr[0], myPair);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            hashMap.forEach((key,value) ->
                    {
                        try {
                            ipAddress.set(key);
                            context.write(ipAddress, value);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
            );


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

    public static class AverageReducer extends Reducer<Text, MyPair, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<MyPair> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            int count = 0;
            for (MyPair val : values)
            {
                sum += val.getSum();
                count += val.getCount();
            }
            result.set(sum/count);

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new AverageInMapper(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {


        FileUtils.deleteDirectory(new File(args[1]));
        //FileUtil.fullyDelete(new File(args[1]));
        HadoopUtils.deletePathIfExists(getConf(), args[1]);

        Job job = new Job(getConf(), "Average");
        job.setJarByClass(AverageInMapper.class);

        job.setMapperClass(AverageInMapper.AverageMapper.class);
        job.setReducerClass(AverageInMapper.AverageReducer.class);
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyPair.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }


}
