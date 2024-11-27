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
import java.util.ArrayList;
import java.util.List;


public class ReservationsWithPriceAboveAverage {


    public static class AveragePriceByHotelStarsMapper extends Mapper<Object, Object, IntWritable, ReservationModel> {


        public void map(Object key, Object value, Context context) throws IOException, InterruptedException {

                String[] fields = value.toString().split(",");

                int hotelStars = Integer.parseInt(fields[28].split("\\.")[0]);
                float price = Float.parseFloat(fields[16]);
                String hotelCity = fields[0];
                String hotelID = fields[1];
                String reservationId = fields[3];
                String country = fields[4];
                String reservationStatus = fields[5];
                context.write(new IntWritable(hotelStars), new ReservationModel(new Text(reservationId),new Text(hotelCity),new Text(hotelID),new Text(country),new Text(reservationStatus),new IntWritable(hotelStars),new FloatWritable(price)));

        }
    }


    public static class AveragePriceByHotelStarsReducer extends Reducer<IntWritable, ReservationModel, Text, Text> {

        public void reduce(IntWritable key, Iterable<ReservationModel> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            List<ReservationModel> reservations = new ArrayList<>();

            for (ReservationModel val : values) {
                sum += val.getPrice().get();
                count++;
                ReservationModel reservationModel = new ReservationModel();
                reservationModel.set(val);
                reservations.add(reservationModel);
            }
            float avg = (count > 0) ? sum / count : 0;

            context.write(new Text("reserveId,hotel_city,price,hotelId,country,reservationStatus"), new Text());

          for (ReservationModel reservationModel : reservations) {
                if (reservationModel.getPrice().get() > avg) {
                    String output = reservationModel.getReservationId().toString() + "," + reservationModel.getHotelCity().toString() + "," +  reservationModel.getPrice().toString()+ "," +  reservationModel.getHotelID().toString() + "," +  reservationModel.getCountry().toString() + reservationModel.getReservationStatus().toString();
                    context.write(new Text(output), new Text());
                }

            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Average price by hotel stars");
        job1.setJarByClass(ReservationsWithPriceAboveAverage.class);
        job1.setMapperClass(AveragePriceByHotelStarsMapper.class);
        job1.setReducerClass(AveragePriceByHotelStarsReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(ReservationModel.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }


}
