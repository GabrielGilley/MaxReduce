import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.lib.NLineInputFormat; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class EliminateRedundantReads {
	
	public static int k = 15;
	public static int NUM_MAP_TASKS;
	public static int NUM_REDUCE_TASKS;
	//public static int k1 = k+1;
	//public static int k11 = 1+k+1; 
	public static String N = "N";
	//public static AdditionalUtilities autil = new AdditionalUtilities();
	public static FilterReadsNQuality re = new FilterReadsNQuality();
	public static String delim = ",";
	public static int minFrequency = 0;
	public static int thresholdCoverage = 1;
		
	public static class EliminateRedundantReadsMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
 		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println("In KmerCount.map");
			try {
				String line = value.toString();
				//System.out.println("FileName:" + fileName);
				String kmers[] = getKmers(line);
				int kmersFreq[] = getFrequency(kmers);
				int estimatedReadCoverage = getMedian(kmersFreq);
				if(estimatedReadCoverage < thresholdCoverage) {
					output.collect(new Text(line), new Text("")); //Key = <kmer>, value = fileName
				}
			}
			catch(Exception e) {System.out.println("Exception in EliminateRedundantReads.map"); e.printStackTrace();}
		}
	}
	
	public static int[] getFrequency(String[] kmersInARead) {
		int[] frequency = new int[kmersInARead.length]; //return median
		for (int i = 0; i < frequency.length; i++) {
			frequency[i] = 0; //replace with hazelcast code
		}
		return frequency;
	}
	
	public static int getMedian(int[] frequency) {
		Arrays.sort(frequency);
		int median = frequency.length/2;
	    if (frequency.length%2 == 1) {
	        return frequency[median];
	    } else {
	        return (int)((frequency[median-1] + frequency[median]) / 2.0);
	    }
	}
	
	public static String[] getKmers(String read) {
		System.out.println("In KmerCount.constructKmersFromARead");
		String tmpRead = read;
		String kmers[] = new String[tmpRead.length()-k+1];
		for (int i = 0; i < tmpRead.length()-k+1; i++) {
			kmers[i] = tmpRead.substring(i, i+k);
		}
		return kmers;
	}
	
	public int run(String ip_path, String op_path) {
		//System.out.println("In FrequencySeparation.run");
		try {
			JobConf conf = new JobConf(FrequencySeparation.class);
			conf.setJobName("EliminateRedundantReads");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(EliminateRedundantReadsMap.class);
			//conf.setCombinerClass(KmerCountReduce.class);
			//conf.setReducerClass(FrequencySeparationReduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf, new Path(ip_path));
			FileOutputFormat.setOutputPath(conf, new Path(op_path));
			conf.setNumMapTasks(NUM_MAP_TASKS);
			conf.setNumReduceTasks(NUM_REDUCE_TASKS);
			//conf.setInt("mapred.line.input.format.linespermap", 2000000);
			JobClient.runJob(conf);
		}
		catch(Exception e) {System.out.println("Exception in EliminateRedundantReads.run"); e.printStackTrace();}
		return 1;
	}
 	
	public static void main(String[] args) throws Exception {
		EliminateRedundantReads err = new EliminateRedundantReads();
		if (args.length >= 4){
			NUM_MAP_TASKS = Integer.parseInt(args[2]);
			NUM_REDUCE_TASKS = Integer.parseInt(args[3]);
			if (args.length > 4){
				k = Integer.parseInt(args[4]);
			}
			if (args.length > 5){
				minFrequency = Integer.parseInt(args[5]);
			}
			err.run(args[0], args[1]);
		}
		else {
			System.out.println("Usage: hadoop jar <path to BuildGraph.jar> "
					+ "<input> <output> <num map tasks> <num reduce tasks> <optional: k> <optional: cutoff frequency>");
		}
	}
}