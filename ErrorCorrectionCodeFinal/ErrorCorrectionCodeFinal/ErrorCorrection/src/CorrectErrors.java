/**
 * Correct errors in each read using majority voting
 *  Usage: ./bin hadoop jar CorrectErrors.jar <ipPath> <opPath> <numHazelInstance> <k>
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

public class CorrectErrors extends Configured implements Tool{
	//static HazelcastInstance client;
	//static IMap<String, byte[]> map;
	static int MAX = 99999, MIN = -1;
	static int k = 15;
	static int i = 0;
	static String delim = "\t";
	static String hzConfigFile;
	static Config config;
	public static int numInstances = 1;
	public static String argfname = "";
	public static int[] finalStats = new int[400];
	public static CorrectErrors ce = new CorrectErrors();
	public static class CorrectErrorsMap extends Mapper<LongWritable, Text, Text, Text> {
		private HazelcastInstance client;
		private IMap<String, String> map;
		//private Text kmer = new Text();
		public static String N = "N";
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			//System.out.println("In KmerCount.map");
			try {
				Configuration contConf = context.getConfiguration();
				int k = Integer.parseInt(contConf.get("kmerLength"));
				int numInstances = Integer.parseInt(contConf.get("numHazelInstances"));
				String line = value.toString();
				//System.out.println("FileName:" + fileName);
				//if (kmc.isReadLine(line, lineNo) == true) {
					String partsIDnRnEBnRC[] = line.split("\t"); //<RID>/<FID>:<Read>\t<suspiciousBase>
					//String sb = partsIDnRnSB[1].replace("suspiciousBaseLocs=", "");
					String fId = partsIDnRnEBnRC[0]; String rId = partsIDnRnEBnRC[1];
					String rd = partsIDnRnEBnRC[2];
					if (partsIDnRnEBnRC.length == 3) {context.write(new Text (fId + "\t" + rId + "\t" + rd), new Text(""));} //if no error just print
					else { //Correct otherwise
					String strErrorBases = partsIDnRnEBnRC[3]; String[] tmpErrorBase = strErrorBases.split(",");
					int[] errorBase = new int[tmpErrorBase.length];
					String strReplacedBaseCount = partsIDnRnEBnRC[4]; 
					for (int i = 0; i < tmpErrorBase.length; i++) {
						errorBase[i]= Integer.parseInt(tmpErrorBase[i]);
					}
					String[] replacedBaseCount = strReplacedBaseCount.split(",");
					//for (int i = 0; i < errorBase.length; i++) {
					//	System.out.println(errorBase[i]);
					//}
					String strReplacedBaseCountOut = ""; 
					for (int i = 0; i < errorBase.length; i++) {
						if (errorBase[i] > k) {
						String[] indReplacedBaseCount = replacedBaseCount[i].split("#");
						double countA = Double.parseDouble(indReplacedBaseCount[0]);
						double countT = Double.parseDouble(indReplacedBaseCount[1]);
						double countG = Double.parseDouble(indReplacedBaseCount[2]);
						double countC = Double.parseDouble(indReplacedBaseCount[3]);
						String[] kmersToChange = ce.generateKmers(rd, errorBase[i], k);
						//for (int j = 0; j < kmersToChange.length; j++) {
						//	System.out.println(kmersToChange[j]);
						//}
						String[] kmersChangedWithA = ce.changeKmers(kmersToChange, 'A', k);
						String[] kmersChangedWithT = ce.changeKmers(kmersToChange, 'T', k);
						String[] kmersChangedWithG = ce.changeKmers(kmersToChange, 'G', k);
						String[] kmersChangedWithC = ce.changeKmers(kmersToChange, 'C', k);
						
						for (int j = 0; j < kmersToChange.length; j++) {
							String a = map.get(kmersChangedWithA[j]); if (a != null) countA += Double.parseDouble(a);
							String t = map.get(kmersChangedWithT[j]); if (t != null) countT += Double.parseDouble(t);
							String g = map.get(kmersChangedWithG[j]); if (g != null) countG += Double.parseDouble(g);
							String c = map.get(kmersChangedWithC[j]); if (c != null) countC += Double.parseDouble(c);
						}
						replacedBaseCount[i] = countA + "#" + countT + "#" + countG + "#" + countC;
					}
					//for (int l = 0; l < replacedBaseCount.length; l++) {
						strReplacedBaseCountOut += "," + replacedBaseCount[i];
					//}
					}
					//strReplacedBaseCountOut = strReplacedBaseCount.replaceFirst(",", "");
					context.write(new Text(fId + "\t" + rId + "\t" + rd), new Text(strErrorBases + "\t" + strReplacedBaseCountOut));
					}
				//}
			}
			catch(Exception e) {System.out.println("Exception in GetKmerCountNLocations.map"); e.printStackTrace();}
 		}
 		@Override
 		protected void setup(Context context){
 			//System.out.printf("Starting mapper with %d instances\n", numInstances);
 			int port = 5701 + context.getTaskAttemptID().getId()%numInstances;
 			//System.out.println("Connecting to port " + port);
 			ClientConfig clientConfig = new ClientConfig();
 			clientConfig.addAddress("127.0.0.1:" + port);
 			this.client = HazelcastClient.newHazelcastClient(clientConfig);
 			this.map = client.getMap("kmer");
 		}
 		@Override
 		protected void cleanup(Context context){
 			//System.out.println("Shutting down client");
 			this.client.shutdown();
 		}
	}
	
	String[] generateKmers1(String rd, int errorBase) {
		String kmers[] = new String[k];
		for (int i = 0; i < errorBase; i++) {
			kmers[i] = "NNN";
		}
		if (errorBase < k) {
			for (int i = 0; i < errorBase; i++) {
				kmers[i] = rd.substring(i, errorBase+i);
			}
		}
		else {
			for (int i = 0; i < k; i++) {
				kmers[i] = rd.substring(errorBase-k+i, errorBase+i);
			}
		}
		return kmers;
	}
	
	String[] generateKmers(String rd, int errorBase, int k) {
		String kmers[] = new String[k];
		//System.out.println(rd);
		//if (errorBase >= k) {
		for (int i = 0; i < k; i++) {
			if (errorBase <= k) {kmers[i] = "NNN";}
			else {kmers[i] = rd.substring(errorBase-k+i, errorBase+i);}
		}
		//}
		return kmers;
	}
	String[] changeKmers(String[] kmersToChange, char base, int k) {
		String changedKmers[] = new String[k];
		for (int i = 0; i < k; i++) {
			 if (kmersToChange[i] != null) {
			char[] charChangedKmers = kmersToChange[i].toCharArray();
			//for (int j = 0; j < charChangedKmers.length; j++) System.out.println(charChangedKmers[i]);
			charChangedKmers[k-1-i] = base;
			changedKmers[i] = new String(charChangedKmers);
			 }
		}
		return changedKmers;
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("numHazelInstances", args[2]);
		conf.set("kmerLength", args[3]);
		Job job = new Job(conf, "CorrectErrorsInReads");
		job.setJarByClass(LoadKmerToDHT.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CorrectErrorsMap.class);
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
		ToolRunner.run(new Configuration(), new CorrectErrors(), args);
	}
}