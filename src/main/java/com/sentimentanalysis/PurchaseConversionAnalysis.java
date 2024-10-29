package com.sentimentanalysis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PurchaseConversionAnalysis {

    // Mapper for User Activity
    public static class UserActivityMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text productID = new Text();
        private Text activity = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // Skip header or malformed rows
            if (fields.length < 4 || fields[0].equals("LogID")) {
                return;
            }

            String productIDStr = fields[3];
            String activityType = fields[2];

            productID.set(productIDStr);
            activity.set("interaction:" + activityType);
            context.write(productID, activity);
        }
    }

    // Mapper for Transactions
    public static class TransactionsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text productID = new Text();
        private Text purchase = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // Skip header or malformed rows
            if (fields.length < 6 || fields[0].equals("TransactionID")) {
                return;
            }

            String productIDStr = fields[3];
            String productCategory = fields[2];
            String quantitySold = fields[4];
            String revenueGenerated = fields[5];

            productID.set(productIDStr);
            purchase.set("purchase:" + productCategory + ":" + quantitySold + ":" + revenueGenerated); 
            context.write(productID, purchase);
        }
    }

    // Reducer Class
    public static class PurchaseConversionReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Integer> categoryInteractionCounts = new HashMap<>();
        private Map<String, Integer> categoryPurchaseCounts = new HashMap<>();
        private Map<String, Double> categoryRevenueTotals = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int interactionCount = 0;
            int purchaseCount = 0;
            double totalRevenue = 0.0;
            String productCategory = "";

            // Process each value for a given productID
            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts[0].equals("interaction")) {
                    interactionCount++;
                } else if (parts[0].equals("purchase")) {
                    purchaseCount++;
                    productCategory = parts[1];
                    totalRevenue += Double.parseDouble(parts[3]);
                }
            }

            // Aggregate results by product category
            categoryInteractionCounts.put(productCategory, categoryInteractionCounts.getOrDefault(productCategory, 0) + interactionCount);
            categoryPurchaseCounts.put(productCategory, categoryPurchaseCounts.getOrDefault(productCategory, 0) + purchaseCount);
            categoryRevenueTotals.put(productCategory, categoryRevenueTotals.getOrDefault(productCategory, 0.0) + totalRevenue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the aggregated results by category
            for (String category : categoryInteractionCounts.keySet()) {
                int totalInteractions = categoryInteractionCounts.get(category);
                int totalPurchases = categoryPurchaseCounts.get(category);
                double totalRevenue = categoryRevenueTotals.get(category);
                double conversionRate = (totalInteractions == 0) ? 0 : (double) totalPurchases / totalInteractions;

                // Output: ProductCategory, ConversionRate, TotalRevenue
                context.write(new Text(category), new Text("ConversionRate: " + conversionRate + ", TotalRevenue: " + totalRevenue));
            }
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        // Set the file paths directly in the code
        String userActivityInput = "/input/datasets/user_activity.csv";
        String transactionsInput = "/input/datasets/transactions.csv";

        // Generate a unique output path based on the current timestamp
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String outputPath = "/output_task_" + timestamp;

        // Create Hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Purchase Conversion Rate");
        job.setJarByClass(PurchaseConversionAnalysis.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PurchaseConversionReducer.class);

        // Set input paths using MultipleInputs
        MultipleInputs.addInputPath(job, new Path(userActivityInput), TextInputFormat.class, UserActivityMapper.class);
        MultipleInputs.addInputPath(job, new Path(transactionsInput), TextInputFormat.class, TransactionsMapper.class);

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.out.println("Output path for this run: " + outputPath);

        // Exit after job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
