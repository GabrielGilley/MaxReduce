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

public class Histogram {
	public static int qualityOffset = 33;
	public static int k = 15;
	public static int NUM_MAP_TASKS;
	public static int NUM_REDUCE_TASKS;
	//public static int k1 = k+1;
	//public static int k11 = 1+k+1; 
	public static String N = "N";
	//public static AdditionalUtilities autil = new AdditionalUtilities();
	public static Histogram histo = new Histogram();
	public static String delim = ",";
	public static int minFrequency = 0;
		
	public static class HistogramMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		String fileName = new String();
		String rId = new String();
		String fId = new String();
		String pos = new String();
		int k = 15;
		int flag = 0;
		int col = 1;
		public void configure(JobConf job)
		{
		   fileName = job.get("map.input.file");
		   String[] tokens = fileName.split("/");
		   fileName = tokens[tokens.length-1];
		   col = Integer.parseInt(job.get("col"));
		}
		
 		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println("In KmerCount.map");
			try {
				String line = value.toString();
				String[] parts = line.split("\t");
				String str = parts[col];
				if (str.contains(".")) {
					str = str.substring(0, str.indexOf('.'));
				}
				output.collect(new Text(parts[col]), new Text("1"));
			}
			catch(Exception e) {System.out.println("Exception in Histogram.map"); e.printStackTrace();}
		}
	}
	
	public static class GetKmerCountNLocationsReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println("In KmerCount.reduce");
			try {
				int frq = 0;
				while (values.hasNext()) {
					frq += Integer.parseInt(values.next().toString());
				}
				output.collect(key, new Text("" + frq));
			}
			catch(Exception e) {System.out.println("Exception in GetKmerCountNLocations.reduce"); e.printStackTrace();}
		}
	}
	
	public int run(String ip_path, String op_path, String numMap, String numReduce, String col) {
		//System.out.println("In KmerCount.run");
		try {
			JobConf conf = new JobConf(Histogram.class);
			conf.setJobName("Histogram");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(HistogramMap.class);
			//conf.setCombinerClass(KmerCountReduce.class);
			conf.setReducerClass(GetKmerCountNLocationsReduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(conf, new Path(ip_path));
			FileOutputFormat.setOutputPath(conf, new Path(op_path));
			conf.setNumMapTasks(Integer.parseInt(numMap));
			conf.setNumReduceTasks(Integer.parseInt(numReduce));
			//conf.setInt("mapred.line.input.format.linespermap", 2000000);
			conf.set("col", col);
			JobClient.runJob(conf);
		}
		catch(Exception e) {System.out.println("Exception in Histogram.run"); e.printStackTrace();}
		return 1;
	}
 	
	public static void main(String[] args) throws Exception {
		Histogram hist = new Histogram();
		//if (args.length >= 4){
			NUM_MAP_TASKS = Integer.parseInt(args[2]);
			NUM_REDUCE_TASKS = Integer.parseInt(args[3]);
			//if (args.length > 4){
				k = Integer.parseInt(args[4]);
				System.out.println("k = " + k);
			//}
			//if (args.length > 5){
				//minFrequency = Integer.parseInt(args[5]);
			//}
			hist.run(args[0], args[1], args[2], args[3], args[4]);
		//}
		//else {
			//System.out.println("Usage: hadoop jar <path to BuildGraph.jar> "
					//+ "<input> <output> <num map tasks> <num reduce tasks> <optional: k> <optional: cutoff frequency>");
		//}
	}
}