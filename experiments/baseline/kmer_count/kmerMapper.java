import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class kmerMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private int k = 7; // Length of k-mer
    private boolean isSequence = false;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Get the k value from the configuration
        k = context.getConfiguration().getInt("kmer.length", 7); // Default k-mer length
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //throw new IOException("grgill is the best");
        String line = value.toString();
        line = line.trim();
        System.out.println("K:" + k);
        if (isSequence) {
            for (int i = 0; i <= line.length() - k; i++) {
                String kmer = line.substring(i, i + k);
                System.out.println(kmer);
                if (kmer.length() == k)
                    context.write(new Text(kmer), new LongWritable(1)); // Emit k-mer with count 1
            }
            isSequence = false; 
        }
        // Process only the sequence lines (every 4th line after the header)
        else if (line.startsWith("@")) {
            isSequence = true;
        }

    }
}

