import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DailyFundFlow {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text date = new Text();
        private Text amount = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    if (fields.length > 7) {
        try {
            Long.parseLong(fields[4]);
            String reportDate = fields[1];
            String totalPurchaseAmt = fields[4];
            String totalRedeemAmt = fields[8];
            date.set(reportDate);
            amount.set(totalPurchaseAmt + "," + totalRedeemAmt);
            context.write(date, amount);
        } catch (NumberFormatException e) {

        }
    }
}

}

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalPurchase = 0;
            long totalRedeem = 0;
            for (Text val : values) {
                String[] amounts = val.toString().split(",");
                totalPurchase += Long.parseLong(amounts[0]);
                totalRedeem += Long.parseLong(amounts[1]);
            }
            result.set(totalPurchase + "," + totalRedeem);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "daily fund flow");
        job.setJarByClass(DailyFundFlow.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

