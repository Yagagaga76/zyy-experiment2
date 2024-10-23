import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FinanceAnalysis {

    public static class BalanceMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 9 && isNumeric(fields[4]) && isNumeric(fields[8])) {
                outputKey.set(fields[1]); // Date
                outputValue.set("BAL," + fields[3] + "," + fields[4] + "," + fields[8]); // Balance, Purchase, Redeem
                context.write(outputKey, outputValue);
            }
        }

        private boolean isNumeric(String str) {
            try {
                Double.parseDouble(str);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    public static class ShiborMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 3 && isNumeric(fields[3])) {
                outputKey.set(fields[0]); // Date
                outputValue.set("SHI," + fields[3]); // Interest rate
                context.write(outputKey, outputValue);
            }
        }

        private boolean isNumeric(String str) {
            try {
                Double.parseDouble(str);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    public static class AnalysisReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalBalance = 0;
            long totalPurchase = 0;
            long totalRedeem = 0;
            double interestRate = -1;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("SHI")) {
                    interestRate = Double.parseDouble(parts[1]);
                } else if (parts[0].equals("BAL")) {
                    totalBalance += Long.parseLong(parts[1]);
                    totalPurchase += Long.parseLong(parts[2]);
                    totalRedeem += Long.parseLong(parts[3]);
                }
            }

            if (interestRate != -1) {
                String range;
                if (interestRate < 3) {
                    range = "0-3%";
                } else if (interestRate < 5) {
                    range = "3-5%";
                } else {
                    range = "5%以上";
                }

                String result =  "Range: " + range + ", Total Balance: " + totalBalance + ", Total Purchase: " + totalPurchase + ", Total Redeem: " + totalRedeem;
                context.write(new Text(key), new Text(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "finance analysis");
        job.setJarByClass(FinanceAnalysis.class);
        job.setReducerClass(AnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, BalanceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ShiborMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

