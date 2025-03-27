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

public class ConfirmErrorLocationInKmer extends Configured implements Tool{
	//static HazelcastInstance client;
	//static IMap<String, byte[]> map;
	static int k = 15;
	static int i = 0;
	static String delim = "\t";
	static String hzConfigFile;
	static Config config;
	public static int numInstances = 1;
	public static String argfname = "";
	public static int[] confirmedErrorBasePosInRd = new int[100];
	public static class ConfirmErrorLocationInKmerMap extends Mapper<LongWritable, Text, Text, Text> {
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
					String partsIDnRnSB[] = line.split("\t"); //<RID>/<FID>:<Read>\t<quality>
					String sbPosInRd = partsIDnRnSB[3];
					String rd = partsIDnRnSB[2];
					rId = partsIDnRnSB[1]; fId = partsIDnRnSB[0];
					
					String[] partsSbPos = sbPosInRd.split(",");
					int[] sbPos = new int[partsSbPos.length];
					for (int i = 0; i < partsSbPos.length; i ++ ) {sbPos[i] = Integer.parseInt(partsSbPos[i]);}
					
					String kmersNPos[] = getKmersNPos(rd);
					
					int numKmer = kmersNPos.length;
					//String[] partsKmerNPos = new String[2];
					String kmers[] = new String[numKmer];
					String posKmerInRd[] = new String[numKmer];
					int freq[] = new int[numKmer];
					for (int i = 0; i < numKmer; i++) {kmers[i] = ""; posKmerInRd[i] = ""; freq[i] = 0;}
					int j = 0;
					for (int i = 0; i < numKmer; i++) {
						//if (!kmersNPos[i].contains(N)) {//other logic required for getAll()
							String partsKmerNPos[] = kmersNPos[i].split("#");
							kmers[i] = partsKmerNPos[0];
							posKmerInRd[i] = partsKmerNPos[1];
							//System.out.println(kmers[i] + "=" + map.get(kmers[i]));
							String tempFreq = map.get(kmers[i]);
							if (!(tempFreq == null)) {
								freq[i] = Integer.parseInt(tempFreq); 
							}//replace with map.getBulk() for more efficiency //uncomment
						//}
					}
					long avgFreq = getAvg(freq); //possibly replace with getMedian(freq);
					int lowFreqKmerThreshold = 2, //check if low frequency
						highCovReadThreshold = 2; //Check if low frequency from low coverage area
					for (int i = 0; i < confirmedErrorBasePosInRd.length; i++) {
						confirmedErrorBasePosInRd[i] = 0;
					}
					for (int i = 0; i < numKmer; i++) {
						if (!kmers[i].contains(N)) {
							if (freq[i] < lowFreqKmerThreshold) { //uncomment
								if ((freq[i]-avgFreq) < highCovReadThreshold) {//uncomment 
									confirmSuspBasesPosInRead(kmers[i].toCharArray(), Integer.parseInt(posKmerInRd[i]), rd.toCharArray(), sbPos);
								}//uncomment
							}//uncomment
						}
					}
					String strConfirmedErrorBasePosInRd = "";
					String substituteCount = ""; 
					for (int i = 0; i < confirmedErrorBasePosInRd.length; i++) {
						if (confirmedErrorBasePosInRd[i] > 0) {
							strConfirmedErrorBasePosInRd += "," + i;
							substituteCount += ",0#0#0#0";
						}
					}
					strConfirmedErrorBasePosInRd = strConfirmedErrorBasePosInRd.replaceFirst(",", "");
					substituteCount = substituteCount.replaceFirst(",", "");
					context.write(new Text(fId+"\t"+rId+"\t"+rd), new Text(strConfirmedErrorBasePosInRd+"\t"+substituteCount));
 			}
 			catch(Exception e) {System.out.println("Exception in ErrorDetectionByFreq.map"); e.printStackTrace();}
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
 		
 		public String[] getKmersNPos(String read) {
 			//System.out.println("In GetKmerCountNFreq.getKmers");
 			int len = read.length();
 			String tmpRead = read;
 			String kmersNFreq[] = new String[tmpRead.length()-k+1];
 			for (int j = 0; j < len-k+1; j++) {
 				kmersNFreq[j] = tmpRead.substring(j, j+k) + "#" + j;
 			}
 			return kmersNFreq;
 		}
 		
 		public static long getAvg(int[] intList) {
 			long sum = 0; int avg = 0, len = intList.length;
 			for (int i = 0; i < len; i++) {
 				sum += intList[i];
 			}
 			avg = (int) (sum / len);
 			return avg;
 		}
 		
 		public static long getMedian(int[] intList) {
 			Arrays.sort(intList);
 			int median = intList.length/2;
 		    if (intList.length%2 == 1) {
 		        return intList[median];
 		    } else {
 		        return (int)((intList[median-1] + intList[median]) / 2.0);
 		    }
 		}
 		
 		public static void confirmSuspBasesPosInRead(char[] kmer, int kmerPosInRd, char[] rd, int[] sbPos) {
 			int lenSbPos = sbPos.length;
 			int lenKmer = kmer.length;
 			for (int i = 0; i < lenSbPos; i++) {
 				if ((sbPos[i] >= kmerPosInRd) && (sbPos[i] < (kmerPosInRd+lenKmer))) {
 					//System.out.println("Arghya");
 					confirmedErrorBasePosInRd[sbPos[i]] += 1;
 				}
 			}
 			return;
 		}
 		
 		public static String confirmSuspBasesPosInKmer(char[] kmer, int kmerPosInRd, char[] rd, int[] sbPos) {
 			int[] sbPosInKmer = new int[sbPos.length];
 			int lenSbPos = sbPosInKmer.length;
 			int lenKmer = kmer.length;
 			int j = 0;
 			for (int i = 0; i < sbPosInKmer.length; i++) {sbPosInKmer[i] = -1;}
 			for (int i = 0; i < lenSbPos; i++) {
 				if ((sbPos[i] >= kmerPosInRd) && (sbPos[i] < (kmerPosInRd+lenKmer))) {
 					//System.out.println("Arghya");
 					confirmedErrorBasePosInRd[sbPos[i]] += 1;
 					sbPosInKmer[j] = sbPos[i] - kmerPosInRd;
 					j++;
 				}
 			}
 			String stringSbPosInKmer = "";
 			for (int i = 0; i < sbPosInKmer.length; i++) {
 				if (sbPosInKmer[i] > -1) {
 					stringSbPosInKmer += "," + sbPosInKmer[i];
 				}
 			}
 			stringSbPosInKmer = stringSbPosInKmer.replaceFirst(",", "");
 			return stringSbPosInKmer;
 		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "ConfirmErrorLocationInKmer");
		job.setJarByClass(LoadKmerToDHT.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(ConfirmErrorLocationInKmerMap.class);
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
		//	FileUtils.forceDelete(outdir);
		ToolRunner.run(new Configuration(), new ConfirmErrorLocationInKmer(), args);
	}
}