import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.lib.NLineInputFormat; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FilterReadsNQuality {
	
	public static int k = 15;
	public static int NUM_MAP_TASKS;
	public static int NUM_REDUCE_TASKS;
	//public static int k1 = k+1;
	//public static int k11 = 1+k+1; 
	public static String N = "N";
	//public static AdditionalUtilities autil = new AdditionalUtilities();
	public static EliminateRedundantReads re = new EliminateRedundantReads();
	public static String delim = ",";
	public static int minFrequency = 0;
	public static int thresholdCoverage = 1;
		
	public static class FilterReadLinesOnlyMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
 		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println("In FilterReadLinesOnly.map");
			try {
				String line = value.toString();
				//System.out.println("FileName:" + fileName);
				if(isReadLine(line) == true) {
					//String parts[] = line.split(" ");
					//char read[] = parts[0].toCharArray();
					//char quality[] = parts[1].toCharArray();
					//int avgQuality = getAvgQuality(quality);
					//String suspiciousBaseLocs = getSuspiciousBaseLocs (read, quality, avgQuality);
					//output.collect(new Text(line), new Text("")); //Key = <kmer>, value = fileName
					String partsIDnRnQ[] = line.split(" "); //<RID>/<FID>:<Read>\t<quality>
					String q = partsIDnRnQ[1];
					String partsIDnR[] = partsIDnRnQ[0].split(":");
					String rd = partsIDnR[1];
					String RIDnFID[] = partsIDnR[0].split("/");
					String rId = RIDnFID[0]; String fId = RIDnFID[1];
					output.collect(new Text(fId+"\t"+rId+"\t"+rd+"\t"+q), new Text(""));
				}
			}
			catch(Exception e) {System.out.println("Exception in FilterReadLinesOnly.map"); e.printStackTrace();}
		}
	}
	
	public static boolean isReadLine(String line) {
		//System.out.println("In FilterReadLinesOnly.isReadLine");
		//if (line.matches("[ATGCNR1234567890:]+")) {
		//	return true;
		//}
		if (line.startsWith("R")) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public static int getAvgQuality(char[] quality) {
		long sum = 0; int avg = 0, len = quality.length;
		for (int i = 0; i < len; i++) {
			sum += quality[i];
		}
		avg = (int) (sum / len);
		return avg;
	}
	
	public static String getSuspiciousBaseLocs (char[] read, char[] quality, int avgQuality) {
		int len = quality.length;
		String suspiciousBaseLocs = "";
		int threshold = 4;
		for (int i = 0; i < len; i++) {
			if ((quality[i]-avgQuality) < threshold) {
				suspiciousBaseLocs += "," + i;
			}
		}
		suspiciousBaseLocs = suspiciousBaseLocs.replaceFirst(",", "");
		return suspiciousBaseLocs;
	}
	public int run(String ip_path, String op_path) {
		//System.out.println("In FrequencySeparation.run");
		try {
			JobConf conf = new JobConf(FilterReadsNQuality.class);
			conf.setJobName("Read Filtering");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(FilterReadLinesOnlyMap.class);
			//conf.setCombinerClass(FilterReadLinesOnlyReduce.class);
			//conf.setReducerClass(FilterReadLinesOnlyReduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf, new Path(ip_path));
			FileOutputFormat.setOutputPath(conf, new Path(op_path));
			conf.setNumMapTasks(NUM_MAP_TASKS);
			conf.setNumReduceTasks(NUM_REDUCE_TASKS);
			//conf.setInt("mapred.line.input.format.linespermap", 2000000);
			JobClient.runJob(conf);
		}
		catch(Exception e) {System.out.println("Exception in FilterReadLinesOnly.run"); e.printStackTrace();}
		return 1;
	}
 	
	public static void main(String[] args) throws Exception {
		FilterReadsNQuality frlo = new FilterReadsNQuality();
		if (args.length >= 4){
			NUM_MAP_TASKS = Integer.parseInt(args[2]);
			NUM_REDUCE_TASKS = Integer.parseInt(args[3]);
			if (args.length > 4){
				k = Integer.parseInt(args[4]);
			}
			if (args.length > 5){
				minFrequency = Integer.parseInt(args[5]);
			}
			frlo.run(args[0], args[1]);
		}
		else {
			System.out.println("Usage: hadoop jar <path to BuildGraph.jar> "
					+ "<input> <output> <num map tasks> <num reduce tasks> <optional: k> <optional: cutoff frequency>");
		}
	}
}