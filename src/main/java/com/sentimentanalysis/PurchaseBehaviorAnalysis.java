package com.sentimentanalysis;

// import java.io.IOException;
// import java.text.ParseException;
// import java.text.SimpleDateFormat;
// import java.util.Date;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.util.GenericOptionsParser;

// public class PurchaseBehaviorAnalysis {

//     // Mapper Class
//     public static class PurchaseBehaviorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//         private final static IntWritable one = new IntWritable(1);
//         private Text outKey = new Text();

//         @Override
//         protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//             // Split CSV line
//             String[] fields = value.toString().split(",");
//             if (fields.length < 7) {
//                 // If the input line doesn't have at least 7 columns, skip it
//                 System.err.println("Skipping malformed input line: " + value.toString());
//                 return;
//             }

//             // Extract the TransactionTimestamp and ProductCategory
//             String timestamp = fields[6];  // TransactionTimestamp is in the 7th column
//             String productCategory = fields[2];  // ProductCategory is in the 3rd column

//             // Parse the hour from the timestamp
//             try {
//                 String hour = getHourFromTimestamp(timestamp);
//                 if (hour != null) {
//                     // Key format: "Hour-ProductCategory"
//                     outKey.set(hour + "-" + productCategory);
//                     // Emit key (hour-productCategory) and value (1 purchase)
//                     System.out.println("Mapper Emitting: " + outKey.toString() + " -> 1");
//                     context.write(outKey, one);
//                 }
//             } catch (ParseException e) {
//                 System.err.println("Skipping malformed timestamp: " + timestamp);
//                 e.printStackTrace();
//             }
//         }

//         // Function to extract hour from timestamp string in the format "YYYY-MM-DD HH:mm:ss"
//         private String getHourFromTimestamp(String timestamp) throws ParseException {
//             SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//             Date date = inputFormat.parse(timestamp);
//             SimpleDateFormat hourFormat = new SimpleDateFormat("HH");
//             return hourFormat.format(date);
//         }
//     }

//     // Reducer Class
//     public static class PurchaseBehaviorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//         private IntWritable result = new IntWritable();

//         @Override
//         protected void reduce(Text key, Iterable<IntWritable> values, Context context)
//                 throws IOException, InterruptedException {
//             int sum = 0;
//             // Sum up all the purchases for the given key (hour-productCategory)
//             for (IntWritable value : values) {
//                 sum += value.get();
//             }
//             result.set(sum);
//             // Emit the key (hour-productCategory) and the total number of purchases
//             System.out.println("Reducer Emitting: " + key.toString() + " -> " + sum);
//             context.write(key, result);
//         }
//     }

//     // Driver Method
//     public static void main(String[] args) throws Exception {
//         Configuration conf = new Configuration();
//         String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//         if (otherArgs.length != 2) {
//             System.err.println("Usage: PurchaseBehaviorAnalysis <input path> <output path>");
//             System.exit(2);
//         }

//         Job job = Job.getInstance(conf, "Purchase Behavior Analysis");
//         job.setJarByClass(PurchaseBehaviorAnalysis.class);

//         // Set Mapper and Reducer classes
//         job.setMapperClass(PurchaseBehaviorMapper.class);
//         job.setReducerClass(PurchaseBehaviorReducer.class);

//         // Set output key and value types
//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(IntWritable.class);

//         // Set input and output paths
//         FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//         FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

//         // Execute job
//         System.exit(job.waitForCompletion(true) ? 0 : 1);
//     }
// }
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