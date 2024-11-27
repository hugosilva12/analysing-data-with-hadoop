package tead;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AveragePricePerNightPerCity {

    public static class  AveragePricePerNightPerCityMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private final static Text outputKey = new Text();
        private FloatWritable pricePerNight = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            String reservationStatus = fields[5];

            if (reservationStatus.equals("Registado") || reservationStatus.equals("Confirmado")) {
                String hotelCity = fields[0];
                String reservationDate = fields[9];
                String[] dateParts = reservationDate.toString().split("-");
                int numNights = Integer.parseInt(fields[11]);
                float price = Float.parseFloat(fields[16]);
                float result = (float)  price / numNights;
                int month = Integer.parseInt(dateParts[1]);
                int year = Integer.parseInt(dateParts[0]);
                outputKey.set(hotelCity + "-" + month + "-" + year);
                pricePerNight.set(result);
                context.write(outputKey, pricePerNight);

            }
        }

    }

    public static class  AveragePricePerNightPerCityReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

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
        Job job = Job.getInstance(conf, "Average Price Per Night Per City");
        job.setJarByClass(AveragePricePerNightPerCity.class);
        job.setMapperClass(AveragePricePerNightPerCity.AveragePricePerNightPerCityMapper.class);
        job.setReducerClass(AveragePricePerNightPerCity.AveragePricePerNightPerCityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
