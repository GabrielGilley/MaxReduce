/**
 * Get all the Reads without N
 * Usage: ./bin hadoop jar RemoveNs.jar <ipPath> <opPath>
 */

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RemoveNs extends Configured implements Tool{
	public static int qualityOffset = 33;
	public static int k = 15;
	public static int NUM_MAP_TASKS;
	public static int NUM_REDUCE_TASKS;
	//public static int k1 = k+1;
	//public static int k11 = 1+k+1; 
	public static String N = "N";
	//public static AdditionalUtilities autil = new AdditionalUtilities();
	public static Statistics stat = new Statistics(); 
	public static RemoveNs geir = new RemoveNs();
	public static String delim = ",";
	public static int minFrequency = 0;
	public static int threshold = 2;
	public static int MAX = 99999;
	public static class RemoveNsMap extends Mapper<LongWritable, Text, Text, Text> {
		
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			//System.out.println("In RemoveNs.map");
			try {
				String line = value.toString();
				String partsIDnRnQ[] = line.split("\t"); //<FID>\t<RID>:<Read>\t<suspiciousBase>
				String fId = partsIDnRnQ[0]; String rId = partsIDnRnQ[1];
				String rd = partsIDnRnQ[2];
				String q = partsIDnRnQ[3];
					
				if (!rd.contains("N")) {
					context.write(new Text(fId), new Text(rId + "\t" + rd + "\t" + q));
				}
					
 			}
			catch(Exception e) {System.out.println("Exception in RemoveNs.map"); e.printStackTrace();}
 		}
 		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "RemoveNs");
		job.setJarByClass(GetErrors.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(RemoveNsMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		//k = Integer.parseInt(args[2]);
		ToolRunner.run(new Configuration(), new RemoveNs(), args);
	}
}
