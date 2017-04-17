/********************************************************************************************
Author : Lalit Singh
Date : March 07, 2017
Work : This code processes the entire input from hdfs and provide final output from reducer

 *******************************************************************************************/
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class outflow
{
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable>
    {
        //private final static IntWritable one = new IntWritable(1);
        private IntWritable weight = new IntWritable();
        private IntWritable outKey = new IntWritable();
	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
		    String line = value.toString();
		    
		    if(line.length()>1)
		    {
				String [] inLine = line.split("\t");
				int outFlow = Integer.parseInt(inLine[inLine.length-1]);
				weight.set(outFlow);
				outKey.set(Integer.parseInt(inLine[0]));
				context.write(outKey,weight);
		    }
		}
	}
    

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
	            throws IOException, InterruptedException
	    {
	    int total = 0;
		for (IntWritable val : values)
		{
		    total += val.get();
		}
		result.set(total);
		context.write(key,result);
	    }
    }

   
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "nodeWeight");

        job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);
	
        job.setJarByClass(outflow.class);
        job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

	//to increasing the number of reducer uncomment the line below
	//job.setNumReduceTasks(5);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
