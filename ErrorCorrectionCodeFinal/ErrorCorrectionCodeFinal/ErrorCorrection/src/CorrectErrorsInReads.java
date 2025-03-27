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

public class CorrectErrorsInReads extends Configured implements Tool{
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
	public static class CorrectErrorsInReadsMap extends Mapper<LongWritable, Text, Text, Text> {
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
 				//System.out.println(parts[2]);
 				//if (parts[2].contains(argfname)) { //Inefficient
 					//map.put(parts[0], parts[1]);
					String partsIDnRnSB[] = line.split("\t");
					if (partsIDnRnSB.length > 4) {
					String replacementCountForAllSb = partsIDnRnSB[4]; String[] tempReplacementCountForAllSb = replacementCountForAllSb.split(",");
					String sbPosInRead = partsIDnRnSB[3]; String[] tempSbPosInRead = sbPosInRead.split(",");
					String rd = partsIDnRnSB[2];
					rId = partsIDnRnSB[1]; fId = partsIDnRnSB[0];
					int[] intSbPosInRead = new int[tempSbPosInRead.length+1];
					for (int i = 0; i < tempSbPosInRead.length; i++) {intSbPosInRead[i] = Integer.parseInt(tempSbPosInRead[i]);} intSbPosInRead[tempSbPosInRead.length] = MAX;
					for (int i = 0; i < intSbPosInRead.length-1; i++) {
						String[] partsReplacementCount = tempReplacementCountForAllSb[i].split("#");
						int freqA = Integer.parseInt(partsReplacementCount[0]), freqT = Integer.parseInt(partsReplacementCount[1]),
							freqG = Integer.parseInt(partsReplacementCount[2]), freqC = Integer.parseInt(partsReplacementCount[3]);
						//System.out.println("freqA = " + freqA);System.out.println("freqC = " + freqC);
						//System.out.println("intSbPosInRead[i] = " + intSbPosInRead[i]); System.out.println("intSbPosInRead[i+1] = " + intSbPosInRead[i+1]);
						if ((intSbPosInRead[i+1] - intSbPosInRead[i]) > k) {
							String[] kmersWithAnSb = getAllKmersWithAnSb(rd, intSbPosInRead[i]);
							//for (int j = 0; j < kmersWithAnSb.length; j++) {System.out.println(kmersWithAnSb[i]);}
							for (int j = 0; j < kmersWithAnSb.length; j++) {
								if (!kmersWithAnSb[j].equals("")) {
								String[] partsKmerNPos = kmersWithAnSb[j].split("#");
								char[] tempkmersWithAnSb = partsKmerNPos[0].toCharArray();
								int sbPosInKmer = intSbPosInRead[i] - Integer.parseInt(partsKmerNPos[1]);
								//System.out.println("tempkmersWithAnSb.toString() = " + String.valueOf(tempkmersWithAnSb));
								//System.out.println("sbPosInKmer = " + sbPosInKmer);
								tempkmersWithAnSb[sbPosInKmer] = 'A'; String tempFreqA = map.get(String.valueOf(tempkmersWithAnSb));
								if (tempFreqA != null) {freqA += Integer.parseInt(tempFreqA);}System.out.println(String.valueOf(tempkmersWithAnSb) + "=" + freqA);
								tempkmersWithAnSb[sbPosInKmer] = 'T'; String tempFreqT = map.get(String.valueOf(tempkmersWithAnSb)); 
								if (tempFreqT != null) {freqT += Integer.parseInt(tempFreqT);}System.out.println(String.valueOf(tempkmersWithAnSb) + "=" + freqT);
								tempkmersWithAnSb[sbPosInKmer] = 'G'; String tempFreqG = map.get(String.valueOf(tempkmersWithAnSb)); 
								if (tempFreqG != null) {freqG += Integer.parseInt(tempFreqG);}System.out.println(String.valueOf(tempkmersWithAnSb) + "=" + freqG);
								tempkmersWithAnSb[sbPosInKmer] = 'C'; String tempFreqC = map.get(String.valueOf(tempkmersWithAnSb)); 
								if (tempFreqC != null) {freqC += Integer.parseInt(tempFreqC);}System.out.println(String.valueOf(tempkmersWithAnSb) + "=" + freqC);
								}
							}
						}
						finalStats[i*4+0] = freqA; finalStats[i*4+1] = freqT; finalStats[i*4+2] = freqG; finalStats[i*4+3] = freqC;
					}
					String strFinalStats = "";
					for (int i = 0; i < finalStats.length; i++) {
						strFinalStats += "#" + finalStats[i];
						if (i%4 == 3) {
							strFinalStats += ",";
						}
					}
					strFinalStats = strFinalStats.replaceFirst("#", "");
					context.write(new Text(fId+"\t"+rId+"\t"+rd+"\t"+sbPosInRead), new Text(strFinalStats));
					//int[] changedBaseCounts = getChangedBaseCounts(kmer, tempSbPosInKmer);
					//String baseWithMaxCount = getMax(changedBaseCounts);
					//String newRead = changeRead(rd, Integer.parseInt(kmerPosInRd)+Integer.parseInt(tempSbPosInKmer[0]), baseWithMaxCount);
					}
 			}
 			catch(Exception e) {System.out.println("Exception in CorrectErrors.map"); e.printStackTrace();}
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
 		
 		public String[] getAllKmersWithAnSb(String rd, int sbPos) {
 			String[] kmersWithAnSb = new String[k];
 			for (int i = 0; i < k; i++) {kmersWithAnSb[i] = "";}
 			int j = 0;
 			for (int i = sbPos-k+1; i <= sbPos; i++) {
 				if (i > 0 && i < rd.length()-k) {
 					kmersWithAnSb[j] = rd.substring(i, i+k);
 					kmersWithAnSb[j] += "#"+i;
 					j++;
 				}
 			}
 			return kmersWithAnSb;
 		}
 		
 		public int[] getChangedBaseCounts(String kmer, String[] sbPosInKmer) {
 			int[] changedBaseCounts = new int[4];
 			int[] intSbPosInKmer = new int[sbPosInKmer.length];
 			for (int i = 0; i < sbPosInKmer.length; i++) {
 				intSbPosInKmer[i] = Integer.parseInt(sbPosInKmer[i]);
 			}
 			String[] bases = {"A", "T", "G", "C"};
 			String[] chKmers = new String[4];
 			int j = 0;
 			for (String b : bases) {
 				chKmers[j] = kmer.substring(0, intSbPosInKmer[0]-1)+b+kmer.substring(intSbPosInKmer[0]);
 				j++;
 			}
 			for (int i = 0; i < 4; i++) {//replace with getAll
 				changedBaseCounts[i] = Integer.parseInt(map.get(chKmers[i])); 
 			}
 			return changedBaseCounts;
 		}
 		
 		public String getMax(int[] intList) {
 			int max = 0;
 			String[] bases = {"A", "T", "G", "C"};
 			String baseWithMaxCount = "";
 			for (int i = 0; i < intList.length; i++) {
 				if (max < intList[i]) {
 					max = intList[i];
 					baseWithMaxCount = bases[i];
 				}
 			}
 			return baseWithMaxCount;
 		}
 		
 		public String changeRead(String rd, int sbPosInRd, String newBase) {
 			String newRd = rd.substring(0, sbPosInRd)+newBase+rd.substring(sbPosInRd);
 			return newRd;
 		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "CorrectErrorsInReads");
		job.setJarByClass(LoadKmerToDHT.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CorrectErrorsInReadsMap.class);
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
		File outdir = new File(args[1]);
		if (args.length > 2) {
			numInstances = Integer.parseInt(args[2]);
			argfname = args[3];
			k = Integer.parseInt(args[5]);
		}
		System.out.println("K = " + k);
		//if (outdir.exists())
			//FileUtils.forceDelete(outdir);
		ToolRunner.run(new Configuration(), new CorrectErrorsInReads(), args);
	}
}