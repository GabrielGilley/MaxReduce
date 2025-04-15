import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    private int join_chance;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException 
    {
        join_chance = context.getConfiguration().getInt("join_percentage", 10);
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");

        // Check if the line is from Table 1 (USER_ID, PRODUCT_ID, NUMBER)
        if (parts.length == 3) 
        {
            String userId = parts[0].trim();
            String productId = parts[1].trim();
            int number = Integer.parseInt(parts[2].trim());

            // Emit only if NUMBER > 10
            if (number <= join_chance) 
            {
                context.write(new Text(productId), new Text("USER," + userId)); // Emit USER_ID with a prefix
            }
        }
        // Check if the line is from Table 2 (PRODUCT_ID, INFORMATION)
        else if (parts.length == 2) 
        {
            String productId = parts[0].trim();
            String information = parts[1].trim();

            // Emit product information
            context.write(new Text(productId), new Text("INFO," + information)); // Emit PRODUCT_ID with a prefix
        }
    }
}
