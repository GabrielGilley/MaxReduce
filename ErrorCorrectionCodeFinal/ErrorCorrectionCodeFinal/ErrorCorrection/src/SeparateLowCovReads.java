/**
 * Separate reads based upon the median coverage of its kmers
 * Usage: ./bin hadoop jar SeparateLowCovReads <ipPath> <opPath> <threshold>
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

public class SeparateLowCovReads extends Configured implements Tool{
	//public static AdditionalUtilities autil = new AdditionalUtilities();
	public static Statistics stat = new Statistics(); 
	public static SeparateLowCovReads slcr = new SeparateLowCovReads();
	
	public static int MAX = 99999, MIN = -1;
	public static class SeparateLowCovReadsMap extends Mapper<LongWritable, Text, Text, Text> {
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			//System.out.println("In KmerCount.map");
			try {
				Configuration contConf = context.getConfiguration();
				int threshold = Integer.parseInt(contConf.get("threshold"));
				String line = value.toString();
				//System.out.println("FileName:" + fileName);
				//if (kmc.isReadLine(line, lineNo) == true) {
					String partsIDnRnPCOR[] = line.split("\t"); //<RID>/<FID>:<Read>\t<suspiciousBase>
					//String sb = partsIDnRnSB[1].replace("suspiciousBaseLocs=", "");
					String fId = partsIDnRnPCOR[0]; String rId = partsIDnRnPCOR[1];
					String rd = partsIDnRnPCOR[2];
					double mean = Double.parseDouble(partsIDnRnPCOR[3]);
					double med = Double.parseDouble(partsIDnRnPCOR[4]);
					double stdev = Double.parseDouble(partsIDnRnPCOR[5]);
					double coef_pearson = Double.parseDouble(partsIDnRnPCOR[6]);
					String strPCor = partsIDnRnPCOR[7];
					String splitPCor[] = strPCor.split(",");
					if (med < threshold) {
						context.write(new Text(fId + "\t" + rId), new Text(rd));
					}
 			}
			catch(Exception e) {System.out.println("Exception in SeparateLowCovReads.map"); e.printStackTrace();}
 		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("threshold", args[2]);
		Job job = new Job(conf, "SeparateLowCovReads");
		job.setJarByClass(SeparateLowCovReads.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(SeparateLowCovReadsMap.class);
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
		//threshold = Integer.parseInt(args[2]);
		ToolRunner.run(new Configuration(), new SeparateLowCovReads(), args);
	}
}
