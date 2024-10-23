package com.sentimentanalysis;

import java.io.IOException;
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
            // Split the input CSV line by comma
            String[] fields = value.toString().split(",");

            // Skip header or malformed rows
            if (fields.length < 4 || fields[0].equals("LogID")) {
                return;
            }

            // Emit interactions from user_activity.csv
            String productIDStr = fields[3];  // Assuming ProductID is the 4th column
            String activityType = fields[2];  // ActivityType is 3rd column

            productID.set(productIDStr);
            activity.set("interaction:" + activityType); // Tagging the interaction type
            context.write(productID, activity);
        }
    }

    // Mapper for Transactions
    public static class TransactionsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text productID = new Text();
        private Text purchase = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input CSV line by comma
            String[] fields = value.toString().split(",");

            // Skip header or malformed rows
            if (fields.length < 6 || fields[0].equals("TransactionID")) {
                return;
            }

            // Emit purchases from transactions.csv
            String productIDStr = fields[3];  // Assuming ProductID is the 4th column
            String productCategory = fields[2];  // Assuming ProductCategory is the 3rd column
            String quantitySold = fields[4];  // Assuming QuantitySold is the 5th column
            String revenueGenerated = fields[5];  // Assuming RevenueGenerated is the 6th column

            productID.set(productIDStr);
            purchase.set("purchase:" + productCategory + ":" + quantitySold + ":" + revenueGenerated); 
            context.write(productID, purchase);
        }
    }

    // Reducer Class
    public static class PurchaseConversionReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int interactionCount = 0;
            int purchaseCount = 0;
            double totalRevenue = 0.0;
            String productCategory = "";

            // Loop through values for a given productID
            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts[0].equals("interaction")) {
                    interactionCount++;
                } else if (parts[0].equals("purchase")) {
                    purchaseCount++;
                    productCategory = parts[1];  // Extracting product category from purchase
                    totalRevenue += Double.parseDouble(parts[3]);  // Adding up the revenue
                }
            }

            // Calculate conversion rate
            double conversionRate = (interactionCount == 0) ? 0 : (double) purchaseCount / interactionCount;

            // Output: ProductID, ProductCategory, ConversionRate, TotalRevenue
            context.write(key, new Text(productCategory + ", ConversionRate: " + conversionRate + ", TotalRevenue: " + totalRevenue));
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        // Check number of input arguments
        if (args.length != 3) {
            System.err.println("Usage: PurchaseConversionAnalysis <user_activity_input> <transactions_input> <output>");
            System.exit(-1);
        }

        // Create Hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Purchase Conversion Rate");

        // Set job jar and main class
        job.setJarByClass(PurchaseConversionAnalysis.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set reducer class
        job.setReducerClass(PurchaseConversionReducer.class);

        // Handle multiple input paths
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserActivityMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionsMapper.class);

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Exit after job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
