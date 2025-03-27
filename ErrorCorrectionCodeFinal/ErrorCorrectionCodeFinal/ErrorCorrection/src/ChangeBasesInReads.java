/**
 * Change the bases in the reads according to the count
 * Usage: ./bin hadoop jar ChangeBasesInReads.jar <ipPath> <opPath>
 */

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

public class ChangeBasesInReads extends Configured implements Tool{
	static int MAX = 99999, MIN = -1;
	public static class ChangeBasesInReadsMap extends Mapper<LongWritable, Text, Text, Text> {
		String fileName = new String();
		String rId = new String();
		String fId = new String();
		String pos = new String();
		//private Text kmer = new Text();
		public static String N = "N";
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			try {
 				String line = value.toString();
 				String[] parts = line.split("\t");
 				//System.out.println(parts[2]);
 				//if (parts[2].contains(argfname)) { //Inefficient
 					//map.put(parts[0], parts[1]);
					String partsIDnRnSBnCount[] = line.split("\t");
					fId = partsIDnRnSBnCount[0]; rId = partsIDnRnSBnCount[1];
					String rd = partsIDnRnSBnCount[2]; char[] tempRd = rd.toCharArray();
					if (partsIDnRnSBnCount.length < 5) {context.write(new Text(fId + "\t" + rId + "\t" + rd), new Text(""));}
					else {
					String sbPos = partsIDnRnSBnCount[3]; String[] tempSbPos = sbPos.split(",");
					String replacementCountForAllSb = partsIDnRnSBnCount[4]; String[] tempReplacementCounts = replacementCountForAllSb.split(",");
						
					for (int i = 0; i < tempSbPos.length; i++) {
						//System.out.println(" " + rId);
						String partsReplacement[] = tempReplacementCounts[i].split("#");
						int baseCount[] =  new int[partsReplacement.length];
						for (int j = 1; j < partsReplacement.length; j++) {
							baseCount[j] = Integer.parseInt(partsReplacement[j]);
						}
						int pos = getMaxPos(baseCount);
						if (pos == 0) {tempRd[Integer.parseInt(tempSbPos[i])] = 'A';}
						if (pos == 1) {tempRd[Integer.parseInt(tempSbPos[i])] = 'C';}
						if (pos == 2) {tempRd[Integer.parseInt(tempSbPos[i])] = 'G';}
						if (pos == 3) {tempRd[Integer.parseInt(tempSbPos[i])] = 'T';}
					}
					String correctedRd = new String(tempRd);
					context.write(new Text(fId+"\t"+rId+"\t"+correctedRd), new Text(""));
					}
 			}
 			catch(Exception e) {System.out.println("Exception in CorrectErrors.map"); e.printStackTrace();}
 		}
 		
 		public int getMaxPos(int[] intList) {
 			int pos = 0;
 			int max = 0;
 			for (int i = 0; i < intList.length; i++) {
 				if(max < intList[i]) {
 					pos = i;
 					max = intList[i];
 				}
 			}
 			return pos;
 		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "ChangeBasesInReads");
		job.setJarByClass(LoadKmerToDHT.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ChangeBasesInReadsMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		//LoadErrorDetectionByFreq gtd = new LoadErrorDetectionByFreq();
		//File outdir = new File(args[1]);
		//if (args.length > 2) {
		//	numInstances = Integer.parseInt(args[2]);
		//	argfname = args[3];
		//	k = Integer.parseInt(args[5]);
		//}
		//System.out.println("K = " + k);
		//if (outdir.exists())
			//FileUtils.forceDelete(outdir);
		ToolRunner.run(new Configuration(), new ChangeBasesInReads(), args);
	}
}