import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WeeklyTransactionVolume {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text dayOfWeek = new Text();
        private Text amount = new Text();
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        private final Calendar cal = Calendar.getInstance();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) { 
                try {
                    String reportDate = fields[0];
                    Date date = sdf.parse(reportDate);
                    cal.setTime(date);
                    String dayName = new SimpleDateFormat("EEEE", Locale.ENGLISH).format(date);
                    dayOfWeek.set(dayName);
                    amount.set(fields[1]); 
                    context.write(dayOfWeek, amount);
                } catch (Exception e) {
                   
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private TreeMap<Long, String> sortedMap = new TreeMap<>(Collections.reverseOrder());

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sumPurchase = 0;
            long sumRedeem = 0;
            int count = 0;
            for (Text val : values) {
                String[] amounts = val.toString().split(",");
                sumPurchase += Long.parseLong(amounts[0]);
                sumRedeem += Long.parseLong(amounts[1]);
                count++;
            }
            if (count > 0) {
                long avgPurchase = sumPurchase / count;
                long avgRedeem = sumRedeem / count;
                sortedMap.put(avgPurchase, key.toString() + "\t" + avgPurchase + "," + avgRedeem);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Long, String> entry : sortedMap.entrySet()) {
                context.write(new Text(entry.getValue()), null);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weekly transaction volume");
        job.setJarByClass(WeeklyTransactionVolume.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

