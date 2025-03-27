/**
 * Load all the kmers to dht
 * Usage: ./bin hadoop jar LoadKmerToDht.jar <ipPath> <opPath> <numHazelInstances> <fileName>
 */
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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

public class LoadKmerToDHT extends Configured implements Tool{
	//static HazelcastInstance client;
	//static IMap<String, byte[]> map;
	static int i = 0;
	static String delim = "\t";
	static String hzConfigFile;
	static Config config;
	public static int numInstances = 1;
	public static String argfname = "";
	public static class GraphToDHTMap extends Mapper<LongWritable, Text, Text, Text> {
		private HazelcastInstance client;
		private IMap<String, String> map;
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			try {
 				Configuration contConf = context.getConfiguration();
 				int numInstances = Integer.parseInt(contConf.get("numHazelInstances"));
 				String line = value.toString();
 				String[] parts = line.split(delim);
 				//System.out.println(parts[2]);
 				if (parts[2].contains(argfname)) { //Inefficient
 					map.put(parts[0], parts[1]);
 					//map.putAsync(parts[0], parts[1]);
 					context.write(new Text(parts[0]), new Text(parts[1]));
 				}
 			}
 			catch(Exception e) {System.out.println("Exception in GraphToDHT.map"); e.printStackTrace();}
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
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("numHazelInstances", args[2]);
		conf.set("fileName", args[3]);
		Job job = new Job(conf, "LoadKmerToDHT");
		job.setJarByClass(LoadKmerToDHT.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(GraphToDHTMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		//LoadGraphToDHT gtd = new LoadGraphToDHT();
		File outdir = new File(args[1]);
		if (args.length > 2) {
			numInstances = Integer.parseInt(args[2]);
			argfname = args[3];
		}
		//if (outdir.exists())
			//FileUtils.forceDelete(outdir);
		ToolRunner.run(new Configuration(), new LoadKmerToDHT(), args);
	}
}