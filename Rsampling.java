/*
Submitted by -: Divya Gupta (dg3483)
I have assumed K can be stored in memory
I have created two mapper and reducer classes. first mapper and reducer is giving the K-sample defined as input by the user.
Second mapper and reducer is grouping models and counting total number of failures per model.
While running job we need to specify three parameters i.e Input path, output path and No of samples.
Output file for first question is stored with name foutput.txt.
I have run the program in peel HPC cluster with k=100000.
File created with 100000 samples is  with name Sample.txt
Question2 is done on jupyter notebook with name dg3483-midterm-question2
Extra credit is done on jupyter notebook with name dg3483-midterm

Reference-:
https://www.geeksforgeeks.org/how-to-find-top-n-records-using-mapreduce/
*/




// namespace
package edu.nyu.bigdata;

//java dependencies
import java.io.IOException;
import java.util.*;
//import java.util.StringTokenizer;
import java.util.regex.*;

// hadoop dependencies
//  maven: org.apache.hadoop:hadoop-client:x.y.z  (x..z = hadoop version, 3.3.0 here
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Waitable;

import java.io.IOException;

//our program. This program will be 'started' by the Hadoop runtime
public class Rsampling {

    // this is the driver
    public static void main(String[] args) throws Exception {

        // get a reference to a job runtime configuration for this program
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: edu.nyu.bigdata.ReservoirSampling  <in> <out> <k>");
            System.exit(2);
        }
        conf.set("k", otherArgs[2]);
        @SuppressWarnings("deprecation")
        // Jobs for pre-processing input files
        Job job1 = new Job(conf, "PreProcessing");
        job1.setJarByClass(Rsampling.class);
        job1.setMapperClass(SamplingMapper.class);
//        job.setCombinerClass(MyReducer.class);
        job1.setReducerClass(SamplingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"/Ksample"));
        job1.waitForCompletion(true);

        Job job2 = new Job(conf, "ModelFailure");
        job2.setJarByClass(Rsampling.class);
        job2.setMapperClass(MapperModelFailure.class);
//        job.setCombinerClass(MyReducer.class);
        job2.setReducerClass(ReducerModelFailure.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/Ksample"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/faliure"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    // Below is the implementation of single mapper and reducer class for pre-processing files.
    public static class SamplingMapper extends Mapper<Object, Text, Text, Text> {
        private final static Text one = new Text("sampled");
        private final static Text line = new Text();
        PriorityQueue<Map.Entry<Long, String>> q;
        long bucket;
        Random randomNumber = new Random();
        public void setup(Context context) throws IOException, InterruptedException {
            bucket = Long.parseLong(context.getConfiguration().get("k"));
            q= new PriorityQueue<Map.Entry<Long, String>>((a,b)-> { return a.getKey().compareTo(b.getKey());});
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            long r= randomNumber.nextLong();
            String l= value.toString();
            Map.Entry<Long, String> tuple= new AbstractMap.SimpleEntry<Long, String>(r, l);
            if(q.size() < bucket)
            {
                q.add(tuple);
            }
            else if( r > q.peek().getKey())
            {
                q.poll();
                q.add(tuple);
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            while(!q.isEmpty())
            {
                Map.Entry<Long, String> t= new AbstractMap.SimpleEntry<>(q.poll());
                long key1= t.getKey();
                String line1= t.getValue();
                line.set(key1+"\t"+line1);
                context.write(one, line);
            }
        }
    }
    public static class SamplingReducer extends Reducer<Text,Text,Text,Text> {
        PriorityQueue<Map.Entry<Long, String>> qr;
        Text key1 = new Text("");
        Text line = new Text();
        long count = 0;
        long bucket;
        String l;
        public void setup(Context context) throws IOException, InterruptedException {
            bucket = Long.parseLong(context.getConfiguration().get("k"));
            qr= new PriorityQueue<Map.Entry<Long, String>>((a,b)-> { return a.getKey().compareTo(b.getKey());});
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String l = val.toString();
                StringTokenizer itr = new StringTokenizer(l,"\t");
                long k1 = Long.parseLong(itr.nextToken());
                Map.Entry<Long, String> tuple = new AbstractMap.SimpleEntry<Long, String>(k1, itr.nextToken());
                if (qr.size() < bucket) {
                    qr.add(tuple);
                }
                else if( k1 > qr.peek().getKey())
                {
                    qr.poll();
                    qr.add(tuple);
                }
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            while(!qr.isEmpty())
            {
                Map.Entry<Long, String> t= new AbstractMap.SimpleEntry<>(qr.poll());
                String line1= t.getValue();
                line.set(line1);
                context.write(key1,line);
            }
        }
    }
    public static class MapperModelFailure extends Mapper<Object, Text, Text, Text> {
        private Text l= new Text();
        private Text k= new Text("");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line= value.toString().trim();
            StringTokenizer itr1 = new StringTokenizer(line,",");
            String date= itr1.nextToken();
            String serialNo= itr1.nextToken();
            String model= itr1.nextToken();
            String capacity= itr1.nextToken();
            String fail= itr1.nextToken();
            l.set(date+","+serialNo+","+model+","+capacity+","+fail);
            k.set(model);
            context.write(k, l);
        }
    }
    public static class ReducerModelFailure extends Reducer<Text,Text,Text,Text> {
        private Text k= new Text();
        private Text v= new Text();
        Integer sum=0;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val: values)
            {
                StringTokenizer itr1 = new StringTokenizer(val.toString(),",");
                String date= itr1.nextToken();
                String serialNo= itr1.nextToken();
                String model= itr1.nextToken();
                String capacity= itr1.nextToken();
                String fail= itr1.nextToken();
                sum= sum+Integer.parseInt(fail);
            }
            v.set(sum.toString());
            k.set(key);
            context.write(key, v);
        }
    }
}


