/**
 * Separate kmers based on file name from where they originated
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

public class SeparateKmersOfEachFile extends Configured implements Tool{
	
	public static class SeparateKmersOfEachFileMap extends Mapper<LongWritable, Text, Text, Text> {
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			try {
 				Configuration contConf = context.getConfiguration();
 				String line = value.toString();
 				String[] parts = line.split("\t");
 				//System.out.println(parts[2]);
 				String argfname = contConf.get("fileName");
 				if (parts[2].contains(argfname)) { //Inefficient
 					context.write(new Text(parts[0]), new Text(parts[1] + "\t" + parts[2]));
 				}
 			}
 			catch(Exception e) {System.out.println("Exception in SeparateKmersOfEachFile.map"); e.printStackTrace();}
 		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("fileName", args[2]);
		Job job = new Job(conf, "SeparateKmersOfEachFile");
		job.setJarByClass(LoadKmerToDHT.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(SeparateKmersOfEachFileMap.class);
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
		//if (args.length > 2) {
			//numInstances = Integer.parseInt(args[2]);
			//argfname = args[2];
		//}
		//if (outdir.exists())
			//FileUtils.forceDelete(outdir);
		ToolRunner.run(new Configuration(), new SeparateKmersOfEachFile(), args);
	}
}