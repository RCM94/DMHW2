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

public class nodeDegree
{
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable>
    {
        //private final static IntWritable one = new IntWritable(1);
        private final static IntWritable count = new IntWritable(1);
        private IntWritable outKey = new IntWritable();
	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	    String line = value.toString();
	    
	    if(line.length()>1)
	    {
		String [] inLine = line.split("\t");
		for(String s : inLine)
		{
		    outKey.set(Integer.parseInt(s));
		    context.write(outKey,count);
		}
	    }
	}
    }
    

    //public static class Reduce extends Reducer<Text, Text, Text, Text>
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        private IntWritable result = new IntWritable();

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException
	    {
		int cntr=0;
		
		for (IntWritable val : values)
		{
		    cntr+=val.get();
		}
		result.set(cntr);
		context.write(key,result);
	    }
    }

   
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "nodeDegree");

        job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);
	
        job.setJarByClass(nodeDegree.class);
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
