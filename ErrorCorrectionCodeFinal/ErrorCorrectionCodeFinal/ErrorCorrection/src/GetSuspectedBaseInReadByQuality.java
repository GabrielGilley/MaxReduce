import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

public class GetSuspectedBaseInReadByQuality extends Configured implements Tool{
	static int qualityOffset = 33;
	static int k = 3;
	static int i = 0;
	static String delim = "\t";
	static String hzConfigFile;
	static Config config;
	public static int numInstances = 1;
	public static String argfname = "";
	public static class ErrorDetectionByQualityMap extends Mapper<LongWritable, Text, Text, Text> {
		private HazelcastInstance client;
		private IMap<String, String> map;
		String fileName = new String();
		String rId = new String();
		String fId = new String();
		String pos = new String();
		//private Text kmer = new Text();
		public static String N = "N";
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			try {
 				String line = value.toString();
 				String[] parts = line.split(delim);
 				//if (parts[2].contains(argfname)) { //Inefficient
 					//map.put(parts[0], parts[1]);
 					String kmersNPos[] = new String[line.length()-k+2];
					String partsIDnRnQ[] = line.split("\t"); //<RID>/<FID>:<Read>\t<quality>
					String qual = partsIDnRnQ[3];
					String rd = partsIDnRnQ[2];
					rId = partsIDnRnQ[1]; fId = partsIDnRnQ[0];
					
					//kmersNPos = getKmersNPos(rd);
					String suspectedBasesPos = getSuspectedBasesPos(rd.toCharArray(),qual.toCharArray());
					context.write(new Text(fId+"\t"+rId+"\t"+rd), new Text(suspectedBasesPos)); //<FileID \t ReadID \t Read \t SuspectedBasePos>
 			}
 			catch(Exception e) {System.out.println("Exception in ErrorDetectionByFreq.map"); e.printStackTrace();}
 		}
 		
 		public static String getSuspectedBasesPos(char[] rd, char[] qualRd) {
 			String suspBasesPos = "";
 			for (int i = 0; i < rd.length; i++) {
 				double pError = Math.pow(10, (-(qualRd[i]-qualityOffset)/10));
 				if (pError > 0.01) {
 					suspBasesPos += "," + i;
 				}
 			}
 			suspBasesPos = suspBasesPos.replaceFirst(",", "");
 			return suspBasesPos;
 		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "ErrorDetectionByQuality");
		job.setJarByClass(LoadKmerToDHT.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ErrorDetectionByQualityMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		//ErrorDetectionByQuality edbq = new ErrorDetectionByQuality();
		File outdir = new File(args[1]);
		if (args.length > 2) {
			numInstances = Integer.parseInt(args[2]);
			argfname = args[3];
		}
		//if (outdir.exists())
			//FileUtils.forceDelete(outdir);
		ToolRunner.run(new Configuration(), new GetSuspectedBaseInReadByQuality(), args);
	}
}