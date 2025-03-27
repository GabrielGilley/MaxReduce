import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapred.lib.NLineInputFormat; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FrequencySeparation {
	
	public static int k = 15;
	public static int NUM_MAP_TASKS;
	public static int NUM_REDUCE_TASKS;
	//public static int k1 = k+1;
	//public static int k11 = 1+k+1; 
	public static String N = "N";
	//public static AdditionalUtilities autil = new AdditionalUtilities();
	public static FrequencySeparation fs = new FrequencySeparation();
	public static String delim = ",";
	public static int minFrequency = 0;
		
	public static class FrequencySeparationMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
 		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println("In FrequencySeparation.map");
			try {
				String line = value.toString();
				//System.out.println("FileName:" + fileName);
				String parts[] = line.split("\t");
				String frequency = parts[1].substring(5);
				if (frequency.length() == 1) {frequency = "00000"+frequency;}
				if (frequency.length() == 2) {frequency = "0000"+frequency;}
				if (frequency.length() == 3) {frequency = "000"+frequency;}
				if (frequency.length() == 4) {frequency = "00"+frequency;}
				if (frequency.length() == 4) {frequency = "0"+frequency;}
				output.collect(new Text(frequency), new Text("")); //Key = <kmer>, value = fileName
			}
			catch(Exception e) {System.out.println("Exception in FrequencySeparation.map"); e.printStackTrace();}
		}
	}
	
	public int run(String ip_path, String op_path) {
		//System.out.println("In FrequencySeparation.run");
		try {
			JobConf conf = new JobConf(FrequencySeparation.class);
			conf.setJobName("Frequency Separation");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(FrequencySeparationMap.class);
			//conf.setCombinerClass(FrequencySeparationReduce.class);
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
		catch(Exception e) {System.out.println("Exception in KmerCount.run"); e.printStackTrace();}
		return 1;
	}
 	
	public static void main(String[] args) throws Exception {
		FrequencySeparation fs = new FrequencySeparation();
		if (args.length >= 4){
			NUM_MAP_TASKS = Integer.parseInt(args[2]);
			NUM_REDUCE_TASKS = Integer.parseInt(args[3]);
			if (args.length > 4){
				k = Integer.parseInt(args[4]);
			}
			if (args.length > 5){
				minFrequency = Integer.parseInt(args[5]);
			}
			fs.run(args[0], args[1]);
		}
		else {
			System.out.println("Usage: hadoop jar <path to BuildGraph.jar> "
					+ "<input> <output> <num map tasks> <num reduce tasks> <optional: k> <optional: cutoff frequency>");
		}
	}
}