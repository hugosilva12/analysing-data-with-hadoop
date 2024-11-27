package tead;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class AverageOccupancyPerCity {

    public static class OccupancyMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private Text cityYearMonth = new Text();
        private FloatWritable occupancy = new FloatWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            Pattern pattern = Pattern.compile("OccupancyKeyModel\\{hotelCity=(.*), year=(\\d+), month=(\\d+), .*");
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                String city = matcher.group(1);
                String year = matcher.group(2);
                String month = matcher.group(3);

                cityYearMonth.set(city + "-" + year + "-" + month);

                float occupancyValue = Float.parseFloat(line.split(",")[4].replace("}", ""));
                occupancy.set(occupancyValue);

                context.write(cityYearMonth, occupancy);
            }
        }
    }
    public static class OccupancyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable value : values) {
                sum += value.get();
                count++;
            }

            float avg = ((float) sum / count);

            context.write(key, new FloatWritable(avg));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average occupancy per city and month");
        job.setJarByClass(AverageOccupancyPerCity.class);
        job.setMapperClass(OccupancyMapper.class);
        job.setReducerClass(OccupancyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
