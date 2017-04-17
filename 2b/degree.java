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

public class degree
{
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable>
    {
        //private final static IntWritable one = new IntWritable(1);
        private IntWritable inKey = new IntWritable();
        private IntWritable inVal = new IntWritable();
        private IntWritable outKey = new IntWritable();
        private IntWritable outVal = new IntWritable();
	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
		    String line = value.toString();
		    
		    if(line.length()>1)
		    {
				String [] inLine = line.split("\t");
				inKey.set(Integer.parseInt(inLine[0]));
				inVal.set(0);
				outKey.set(Integer.parseInt(inLine[1]));
				outVal.set(1);
				context.write(inKey,inVal);
				context.write(outKey,outVal);
		    }
		}
	}
    

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable[]>
    {
        private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
	            throws IOException, InterruptedException
	    {

	    int out = 0;
	    int in = 0;
		for (IntWritable val : values)
		{
		    if (val.get() == 0)
		    {
		    	out++;
		    }
		    else
		    {
		    	in++;
		    }
		}
		IntWritable [] value = new IntWritable[2];
		value[0].set(in);
		value[1].set(out);
		context.write(key,value);
	    }
    }

   
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "nodeWeight");

        job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);
	
        job.setJarByClass(degree.class);
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
