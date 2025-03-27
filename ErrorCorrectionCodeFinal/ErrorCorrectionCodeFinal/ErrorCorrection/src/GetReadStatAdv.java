/**
 * Calculate the mean, median and standard deviation of all kmers frequency in a read
 *  Usage: ./bin hadoop jar GetReadStat.jar <ipPath> <opPath> 
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class GetReadStatAdv extends Configured implements Tool{
	public static int qualityOffset = 33;
	public static int k = 15;
	public static int NUM_MAP_TASKS;
	public static int NUM_REDUCE_TASKS; 
	public static String N = "N";
	//public static AdditionalUtilities autil = new AdditionalUtilities();
	public static Statistics stat = new Statistics(); 
	public static GetReadStatAdv kmc = new GetReadStatAdv();
	public static String delim = ",";
	public static int minFrequency = 0;
	public static class GetReadStatAdvMap extends Mapper<LongWritable, Text, Text, Text> {
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			try {
				String line = value.toString();
				String partsIDnRnPCOR[] = line.split("\t");
				String fId = partsIDnRnPCOR[0]; String rId = partsIDnRnPCOR[1];
				String rd = partsIDnRnPCOR[2];
				String strPCor = partsIDnRnPCOR[3];
				String splitPCor[] = strPCor.split(",");
				double[] pCor = new double[splitPCor.length];
				for (int i = 0; i < splitPCor.length; i++) {
					pCor[i] = Double.parseDouble(splitPCor[i]);
				}
				double mean = stat.getMean(pCor);
				double med = stat.getMedian(pCor);
				double stdev = stat.getStdDev(pCor);
				context.write(new Text(fId + "\t" + rId + "\t" + rd), 
							new Text(Double.toString(mean) + "\t" + Double.toString(med) + "\t" + Double.toString(stdev) + "\t" + strPCor));
 			}
			catch(Exception e) {System.out.println("Exception in GetKmerCountNLocations.map"); e.printStackTrace();}
 		}
 		
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "GetReadStatAdv");
		job.setJarByClass(LoadKmerToDHT.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(GetReadStatAdvMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new GetReadStatAdv(), args);
	}
}
