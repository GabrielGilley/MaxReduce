/**
 * Get the frequency of each kmer in a read from hash table
 * Usage: ./bin hadoop jar GetReadStat.jar <ipPath> <opPath> <numHazelInstance> <k>
 */

import java.io.File;
import java.io.IOException;
import java.util.*;

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

public class GetReadStat extends Configured implements Tool{
	//static HazelcastInstance client;
	//static IMap<String, byte[]> map;
	static int MAX = 99999, MIN = -1;
	public static String N = "N";
	public static GetReadStat grs = new GetReadStat();
	public static class GetReadStatMap extends Mapper<LongWritable, Text, Text, Text> {
		private HazelcastInstance client;
		private IMap<String, String> map;
		//private Text kmer = new Text();
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			try {
 				Configuration contConf = context.getConfiguration();
 				int k = Integer.parseInt(contConf.get("kmerLength"));
 				int numInstances = Integer.parseInt(contConf.get("numHazelInstances"));
 				String line = value.toString();
 				String[] parts = line.split("\t");
 				String partsIDnRnQ[] = line.split("\t"); //<RID>/<FID>:<Read>\t<suspiciousBase>
				//String sb = partsIDnRnSB[1].replace("suspiciousBaseLocs=", "");
				String q = partsIDnRnQ[3];
				String rd = partsIDnRnQ[2];
				 String fId = partsIDnRnQ[0]; String rId = partsIDnRnQ[1];
				String kmers[] = new String[rd.length()-k+2];
				kmers = grs.getKmersFromRead(rd, k);
				//Set<String> setKmers = new HashSet<String>(Arrays.asList(kmers));
				//Map<String,String> kv = map.getAll(setKmers);
				//Collection<String> collKeyKmers = kv.keySet();
				//Collection<String> collValCounts = kv.values();
				//String[] keyKmers = collKeyKmers.toArray(new String[collKeyKmers.size()]);
				//String[] valCounts = collValCounts.toArray(new String[collValCounts.size()]);
				//String strValCounts = "";
				//for (int i = 0; i < valCounts.length; i++) {
				//	strValCounts += "," + valCounts[i];
				//}
				//strValCounts = strValCounts.replaceFirst(",", "");
				//context.write(new Text(fId + "\t" + rId + "\t" + rd), new Text(strValCounts));
				
				String counts = "";
				for (int i = 0; i < kmers.length; i++) {
					if (!kmers[i].contains("N")) {
						String abundance = map.get(kmers[i]);
						if (abundance == null) {abundance = "0";}
						counts += "," + abundance;
					}
				}
				counts = counts.replaceFirst(",", "");
				context.write(new Text(fId + "\t" + rId + "\t" + rd), new Text(counts));
 			}
 			catch(Exception e) {System.out.println("Exception in CorrectErrors.map"); e.printStackTrace();}
 		}
 		@Override
 		protected void setup(Context context){
 			Configuration contConf = context.getConfiguration();
 			int numInstances = Integer.parseInt(contConf.get("numHazelInstances"));
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
	
	public String[] getKmersFromRead(String read, int k) {
 		//System.out.println("In GetKmerCountNLocations.constructKmersFromARead");
 		int len = read.length();
 		//String tmpRead = read;
 		String kmers[] = new String[read.length()-k+1];
 		for (int i = 0; i < len-k+1; i++) {
 			kmers[i] = read.substring(i, i+k);
 		}
 		return kmers;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("numHazelInstances", args[2]);
		conf.set("kmerLength", args[3]);
		Job job = new Job(conf, "GetReadStat");
		job.setJarByClass(LoadKmerToDHT.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(GetReadStatMap.class);
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
			//numInstances = Integer.parseInt(args[2]);
			//argfname = args[3];
			//k = Integer.parseInt(args[4]);
		//}
		//System.out.println("K = " + k);
		//if (outdir.exists())
			//FileUtils.forceDelete(outdir);
		ToolRunner.run(new Configuration(), new GetReadStat(), args);
	}
}