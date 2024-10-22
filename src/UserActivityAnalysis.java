import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

public class UserActivityAnalysis {

    public static class ActivityMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text userId = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.contains("user_id") || line.contains("direct_purchase_amt")) {
                
                return;
            }
            String[] parts = line.split(",");
            if (parts.length > 9) {
                try {
                    long purchaseAmt = Long.parseLong(parts[5].trim());
                    long redeemAmt = Long.parseLong(parts[9].trim());
                    if (purchaseAmt > 0 || redeemAmt > 0) {
                        userId.set(parts[0]);
                        context.write(userId, one);
                    }
                } catch (NumberFormatException e) {
                    
                    System.err.println("Error parsing data from: " + line + ", error: " + e.getMessage());
                }
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, String> sortedMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            sortedMap = new TreeMap<>(java.util.Collections.reverseOrder()); 
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            sortedMap.put(sum, key.toString()); 
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (java.util.Map.Entry<Integer, String> entry : sortedMap.entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "user activity analysis");
        job.setJarByClass(UserActivityAnalysis.class);
        job.setMapperClass(ActivityMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

