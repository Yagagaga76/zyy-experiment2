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
import java.util.HashMap;
import java.util.Map;

public class InterestImpactAnalysis {

    public static class UserBalanceMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 9 && isNumeric(fields[4]) && isNumeric(fields[8])) {
                String reportDate = fields[1];
                String totalPurchaseAmt = fields[4];
                String totalRedeemAmt = fields[8];
                outputKey.set(reportDate);
                outputValue.set("UBT," + totalPurchaseAmt + "," + totalRedeemAmt);
                context.write(outputKey, outputValue);
            }
        }

        private boolean isNumeric(String str) {
            try {
                Double.parseDouble(str);
                return true;
            } catch(NumberFormatException e) {
                return false;
            }
        }
    }

    public static class ShiborMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 2 && isNumeric(fields[1])) {
                String reportDate = fields[0];
                double interestRate = Double.parseDouble(fields[1]);
                outputKey.set(reportDate);
                outputValue.set("SHI," + interestRate);
                context.write(outputKey, outputValue);
            }
        }

        private boolean isNumeric(String str) {
            try {
                Double.parseDouble(str);
                return true;
            } catch(NumberFormatException e) {
                return false;
            }
        }
    }

    public static class InterestReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, long[]> rateAggregates;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            rateAggregates = new HashMap<>();
        }

@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    long totalPurchase = 0;
    long totalRedeem = 0;
    double interestRate = -1;  

    for (Text val : values) {
        String[] parts = val.toString().split(",");
        if (parts[0].equals("SHI")) {
            interestRate = Double.parseDouble(parts[1]);
        } else if (parts[0].equals("UBT")) {
            totalPurchase += Long.parseLong(parts[1]);
            totalRedeem += Long.parseLong(parts[2]);
        }
    }

    if (interestRate != -1) {  // 仅在找到有效的利率时执行
        String range;
        if (interestRate < 3) {
            range = "0-3%";
        } else if (interestRate < 5) {
            range = "3-5%";
        } else {
            range = "5%以上";
        }

        Text outKey = new Text(range + " " + key.toString()); 
        String result = "Purchase: " + totalPurchase + ", Redeem: " + totalRedeem;
        context.write(outKey, new Text(result));
    }
}


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, long[]> entry : rateAggregates.entrySet()) {
                String rateRange = entry.getKey();
                long[] sums = entry.getValue();
                String result = "Purchase: " + sums[0] + ", Redeem: " + sums[1];
                context.write(new Text(rateRange), new Text(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "interest impact analysis");
        job.setJarByClass(InterestImpactAnalysis.class);
        job.setReducerClass(InterestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserBalanceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ShiborMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

