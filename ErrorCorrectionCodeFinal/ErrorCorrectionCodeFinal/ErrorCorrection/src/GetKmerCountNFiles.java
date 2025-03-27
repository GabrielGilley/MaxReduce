/**
 * Count kmers and index the file from which it belong
 * Usage: ./bin hadoop jar GetKmerCountNFiles.jar <ipPath> <opPath> <numMap> <numReduce> <k> <flagQorF>
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.lib.NLineInputFormat; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class GetKmerCountNFiles {
	//public static int qualityOffset = 33;
	//public static int k = 15;
	public static int NUM_MAP_TASKS;
	public static int NUM_REDUCE_TASKS;
	//public static int k1 = k+1;
	//public static int k11 = 1+k+1; 
	public static String N = "N";
	//public static AdditionalUtilities autil = new AdditionalUtilities();
	public static GetKmerCountNFiles kmc = new GetKmerCountNFiles();
	public static String delim = ",";
	public static int minFrequency = 0;
		
	public static class GetKmerCountNLocationsMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		String fileName = new String();
		String rId = new String();
		String fId = new String();
		String pos = new String();
		int k = 15;
		int flag = 0;
		int qualityOffset = 33;
		public void configure(JobConf job)
		{
		   fileName = job.get("map.input.file");
		   String[] tokens = fileName.split("/");
		   fileName = tokens[tokens.length-1];
		   k = Integer.parseInt(job.get("kmerLength"));
		   flag = Integer.parseInt(job.get("flagQorF"));
		   qualityOffset = Integer.parseInt(job.get("qOffset"));
		}
		
 		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println("In KmerCount.map");
			try {
				String line = value.toString();
				//System.out.println("FileName:" + fileName);
				//if (kmc.isReadLine(line, lineNo) == true) {
					String kmersNPos[] = new String[line.length()-k+2];
					String partsIDnRnQ[] = line.split("\t"); //<RID>/<FID>:<Read>\t<suspiciousBase>
					//String sb = partsIDnRnSB[1].replace("suspiciousBaseLocs=", "");
					String q = partsIDnRnQ[3];
					String rd = partsIDnRnQ[2];
					rId = partsIDnRnQ[1]; fId = partsIDnRnQ[0];
					double[] pCorrect = kmc.getProbCorBase(rd.toCharArray(), q.toCharArray(), qualityOffset);
					kmersNPos = kmc.getKmersNPosFromRead(rd, k);
					
					for (int i = 0; i < kmersNPos.length; i++) {
						String partsKmerNPos[] = kmersNPos[i].split("#");
						String kmer = partsKmerNPos[0];
						int kmerPos = Integer.parseInt(partsKmerNPos[1]) ;
						double cumPCorrect = 1;
						if (flag == 1) {
							for (int j = kmerPos; j < kmerPos+k; j++) {
								cumPCorrect *= pCorrect[j];
							}
						}
						if (!kmer.toString().contains(N)) {
							//System.out.println(kmer);
							output.collect(new Text(kmer), new Text(cumPCorrect + delim + fId )); //Key = <kmer>, value = fileName
						}
					}
				//}
			}
			catch(Exception e) {System.out.println("Exception in GetKmerCountNLocations.map"); e.printStackTrace();}
		}
	}
	
	public static class GetKmerCountNLocationsReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println("In KmerCount.reduce");
			try {
				int freqPos = 0, fIdPos = 1;//, rIdPos = 2, posPos = 3;
				double freq = 0;
				String fId = "";//, rId = "", pos = "";
				while (values.hasNext()) {
					String tmpVal =  values.next().toString();
					//System.out.println(tmpVal);
					String[] tokens = tmpVal.split(delim);
					freq += Double.parseDouble(tokens[freqPos]); //First token is the count
					if (!fId.contains(tokens[fIdPos])) {
						fId += "," + tokens[fIdPos];
					}
					fId = fId.replaceFirst(",", "");
					//readId += tokens[rIdPos] + ",";
					//pos += tokens[posPos] + ",";
				}
				
				output.collect(key, new Text(freq + "\t" + fId)); //<kmer \t frequency \t fileID>
				
			}
			catch(Exception e) {System.out.println("Exception in GetKmerCountNLocations.reduce"); e.printStackTrace();}
		}
	}
	public static double[] getProbCorBase(char[] rd, char[] qualRd, int qualityOffset) {
		String suspBasesPos = "";
		double[] pCorrect = new double[rd.length];
		for (int i = 0; i < rd.length; i++) {
			double pError = Math.pow(10, (-(qualRd[i]-qualityOffset)/10));
			pCorrect[i] = 1 - pError;
		}
		return pCorrect;
	}
	public String[] getKmersNPosFromRead(String read, int k) {
		//System.out.println("In GetKmerCountNLocations.constructKmersFromARead");
		int len = read.length();
		//String tmpRead = read;
		String kmersNPos[] = new String[read.length()-k+1];
		for (int i = 0; i < len-k+1; i++) {
			kmersNPos[i] = read.substring(i, i+k) + "#" + Integer.toString(i);
		}
		return kmersNPos;
	}
		
	public boolean isReadLine(String line, int lineNo, int k) {
		//System.out.println("In KmerCount.isReadLine");
		if (line.matches("[ATGCNR1234567890:]+") && line.length()>=k+1) {
			return true;
		}
		else {
			return false;
		}
		/*if ((lineNo % 4) == 1) {
			return true;
		}
		else {
			return false;
		}*/
	}
	
	public int run(String ip_path, String op_path, String numMap, String numReduce, String kmerLength, String flagQorF, String qOffset) {
		//System.out.println("In KmerCount.run");
		try {
			JobConf conf = new JobConf(GetKmerCountNFiles.class);
			conf.setJobName("KmerCount");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(GetKmerCountNLocationsMap.class);
			//conf.setCombinerClass(KmerCountReduce.class);
			conf.setReducerClass(GetKmerCountNLocationsReduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf, new Path(ip_path));
			FileOutputFormat.setOutputPath(conf, new Path(op_path));
			conf.setNumMapTasks(Integer.parseInt(numMap));
			conf.setNumReduceTasks(Integer.parseInt(numReduce));
			//conf.setInt("mapred.line.input.format.linespermap", 2000000);
			conf.set("kmerLength", kmerLength);
			conf.set("flagQorF", flagQorF);
			conf.set("qOffset", qOffset);
			JobClient.runJob(conf);
		}
		catch(Exception e) {System.out.println("Exception in KmerCount.run"); e.printStackTrace();}
		return 1;
	}
 	
	public static void main(String[] args) throws Exception {
		GetKmerCountNFiles gknl = new GetKmerCountNFiles();
		//if (args.length >= 4){
			NUM_MAP_TASKS = Integer.parseInt(args[2]);
			NUM_REDUCE_TASKS = Integer.parseInt(args[3]);
			//if (args.length > 4){
				//k = Integer.parseInt(args[4]);
				//System.out.println("k = " + k);
			//}
			//if (args.length > 5){
				//minFrequency = Integer.parseInt(args[5]);
			//}
			gknl.run(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
		//}
		//else {
			//System.out.println("Usage: hadoop jar <path to BuildGraph.jar> "
					//+ "<input> <output> <num map tasks> <num reduce tasks> <optional: k> <optional: cutoff frequency>");
		//}
	}
}