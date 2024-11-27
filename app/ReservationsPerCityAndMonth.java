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


public class ReservationsPerCityAndMonth {

    public static class ReservationsPerCityAndMonthMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static Text outputKey = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            String reservationStatus = fields[5];

            if (reservationStatus.equals("Registado") || reservationStatus.equals("Confirmado")) {
                String hotelCity = fields[0];
                String reservationDate = fields[9];
                String[] dateParts = reservationDate.toString().split("-");
                int month = Integer.parseInt(dateParts[1]);
                int year = Integer.parseInt(dateParts[0]);
                outputKey.set(hotelCity + "-" + month + "-" + year);
                context.write(outputKey, one);

            }
        }

    }

    public static class ReservationsPerCityAndMonthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();

            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Reservations price per city and month");
        job.setJarByClass(ReservationsPerCityAndMonth.class);
        job.setMapperClass(ReservationsPerCityAndMonthMapper.class);
        job.setReducerClass(ReservationsPerCityAndMonthReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
