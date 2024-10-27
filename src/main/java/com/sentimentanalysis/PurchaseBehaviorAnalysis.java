package com.sentimentanalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class PurchaseBehaviorAnalysis {

    public static class Task3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            
            if (fields.length == 7) {  // Ensure the correct number of fields
                String productCategory = fields[2];  // Extract ProductCategory
                String timestamp = fields[6];  // Extract TransactionTimestamp

                try {
                    // Parse the timestamp and extract the hour
                    LocalDateTime transactionDate = LocalDateTime.parse(timestamp, formatter);
                    int hour = transactionDate.getHour();  // Extract hour

                    // Emit ProductCategory as key and the hour as the value
                    context.write(new Text(productCategory), new IntWritable(hour));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class Task3Reducer extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Integer> hourCountMap = new HashMap<>();

            // Count occurrences of each hour
            for (IntWritable value : values) {
                int hour = value.get();
                hourCountMap.put(hour, hourCountMap.getOrDefault(hour, 0) + 1);
            }

            // Find the hour with the maximum count for this product category
            int peakHour = -1;
            int maxCount = 0;
            for (Map.Entry<Integer, Integer> entry : hourCountMap.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    peakHour = entry.getKey();
                }
            }

            // Emit the ProductCategory and the most popular hour along with the count of purchases in that hour
            context.write(key, new Text("Most Popular Hour: " + peakHour + " with " + maxCount + " purchases"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PurchaseBehaviorAnalysis");
        job.setJarByClass(PurchaseBehaviorAnalysis.class);
        job.setMapperClass(Task3Mapper.class);
        job.setReducerClass(Task3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}