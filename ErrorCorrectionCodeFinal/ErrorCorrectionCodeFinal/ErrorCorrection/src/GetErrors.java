/**
 * Get erroneous bases from high coverage reads
 * Usage: ./bin hadoop jar GetErrors.jar <ipPath> <opPath> <threshold>
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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetErrors extends Configured implements Tool{
	public static Statistics stat = new Statistics(); 
	public static GetErrors ge = new GetErrors();
	public static int MAX = 99999, MIN = -1;
	public static class GetErrorsMap extends Mapper<LongWritable, Text, Text, Text> {
 		public void map(LongWritable key, Text value, Context context) throws IOException {
 			//System.out.println("In KmerCount.map");
			try {
				Configuration contConf = context.getConfiguration();
				int threshold = Integer.parseInt(contConf.get("threshold"));
				String line = value.toString();
				//System.out.println("FileName:" + fileName);
				//if (kmc.isReadLine(line, lineNo) == true) {
					//String kmersNPos[] = new String[line.length()-k+2];
					String partsIDnRnPCOR[] = line.split("\t"); //<RID>/<FID>:<Read>\t<suspiciousBase>
					//String sb = partsIDnRnSB[1].replace("suspiciousBaseLocs=", "");
					String fId = partsIDnRnPCOR[0]; String rId = partsIDnRnPCOR[1];
					String rd = partsIDnRnPCOR[2];
					double mean = Double.parseDouble(partsIDnRnPCOR[3]);
					double med = Double.parseDouble(partsIDnRnPCOR[4]);
					double stdev = Double.parseDouble(partsIDnRnPCOR[5]);
					//double coef_pearson = Double.parseDouble(partsIDnRnPCOR[6]);
					String strPCor = partsIDnRnPCOR[6];
					String splitPCor[] = strPCor.split(",");
					double[] pCor = new double[splitPCor.length];
					for (int i = 0; i < splitPCor.length; i++) {
						pCor[i] = Double.parseDouble(splitPCor[i]);
					}
					if (med >= threshold) {
					int[] errors = ge.getErrors(pCor, threshold);
					String strError = "";
					String strReplacedBaseCounts = "";
					for (int i = 0; i < errors.length; i++) {
						if (errors[i] != MIN) {
						strError += "," + Integer.toString(errors[i]);
						strReplacedBaseCounts += "," + "0#0#0#0"; //initialize count for ATGC
						}
					}
					strError = strError.replaceFirst(",", "");
					strReplacedBaseCounts = strReplacedBaseCounts.replaceFirst(",", "");
					context.write(new Text(fId + "\t" + rId + "\t" + rd), new Text(strError + "\t" + strReplacedBaseCounts));
					}
				//}
 			}
			catch(Exception e) {System.out.println("Exception in GetErrors.map"); e.printStackTrace();}
 		}
	}
	
	int[] getErrors(double[] pCor, double threshold) {
		int[] errors = new int[pCor.length];
		for (int i = 0; i < errors.length; i++) {
			errors[i] = -1;
		}
		int j = 0;
		for (int i = 0; i < errors.length; i++) {
			if (pCor[i] < threshold) {
				errors[j] = i;
			}
			else {
				j++;
			}
		}
		return errors;
	}
	
	//int[] getErrorBases(String rd, int[] errorKmerStartLoc) {
		/////////////////////////Alternative method by sorting//////////////////////////////////////////
		//Pair[] errorBasesCount = new Pair[rd.length()];
		//for (int i = 0; i < errorBasesCount.length; i++) {
		//	errorBasesCount[i] = new Pair(i, 0);						
		//}
		//for (int i = 0; i < errorKmerStartLoc.length; i++) {
		//	for (int j = 0; j < k; j++) {
		//		errorBasesCount[errorKmerStartLoc[i]+j].value++;
		//	}
		//}
		//Arrays.sort(errorBasesCount);
		///////////////////////////////////////////////////////////////////
	//}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("threshold", args[2]);
		Job job = new Job(conf, "GetErrors");
		job.setJarByClass(GetErrors.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(GetErrorsMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new GetErrors(), args);
	}
}
