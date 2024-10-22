package com.sentimentanalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class PurchaseBehaviorAnalysis {

    // Mapper Class
    public static class PurchaseBehaviorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split CSV line
            String[] fields = value.toString().split(",");
            if (fields.length < 7) {
                // If the input line doesn't have at least 7 columns, skip it
                System.err.println("Skipping malformed input line: " + value.toString());
                return;
            }

            // Extract the TransactionTimestamp and ProductCategory
            String timestamp = fields[6];  // TransactionTimestamp is in the 7th column
            String productCategory = fields[2];  // ProductCategory is in the 3rd column

            // Parse the hour from the timestamp
            try {
                String hour = getHourFromTimestamp(timestamp);
                if (hour != null) {
                    // Key format: "Hour-ProductCategory"
                    outKey.set(hour + "-" + productCategory);
                    // Emit key (hour-productCategory) and value (1 purchase)
                    System.out.println("Mapper Emitting: " + outKey.toString() + " -> 1");
                    context.write(outKey, one);
                }
            } catch (ParseException e) {
                System.err.println("Skipping malformed timestamp: " + timestamp);
                e.printStackTrace();
            }
        }

        // Function to extract hour from timestamp string in the format "YYYY-MM-DD HH:mm:ss"
        private String getHourFromTimestamp(String timestamp) throws ParseException {
            SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = inputFormat.parse(timestamp);
            SimpleDateFormat hourFormat = new SimpleDateFormat("HH");
            return hourFormat.format(date);
        }
    }

    // Reducer Class
    public static class PurchaseBehaviorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // Sum up all the purchases for the given key (hour-productCategory)
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            // Emit the key (hour-productCategory) and the total number of purchases
            System.out.println("Reducer Emitting: " + key.toString() + " -> " + sum);
            context.write(key, result);
        }
    }

    // Driver Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: PurchaseBehaviorAnalysis <input path> <output path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Purchase Behavior Analysis");
        job.setJarByClass(PurchaseBehaviorAnalysis.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(PurchaseBehaviorMapper.class);
        job.setReducerClass(PurchaseBehaviorReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // Execute job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
