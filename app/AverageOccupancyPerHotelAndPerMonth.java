package tead;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;


public class AverageOccupancyPerHotelAndPerMonth {

    public static class OccupancyMapper extends Mapper<Object, Object, OccupancyKeyModel, OccupancyValueModel> {

        public void map(Object key, Object value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            String reservationStatus = fields[5];

            if (reservationStatus.equals("Registado") || reservationStatus.equals("Confirmado")) {
                String hotelCity = fields[0];
                String hotelId = fields[1];
                String arrivalDate = fields[9];
                String departureDate = fields[10];
                int numNights = Integer.parseInt(fields[11]);
                int totalRooms = Integer.parseInt(fields[29]);

                String[] dateParts = arrivalDate.toString().split("-");
                int year = Integer.parseInt(dateParts[0]);
                int month = Integer.parseInt(dateParts[1]);


                LocalDate initialDate = LocalDate.parse(arrivalDate);
                LocalDate finalDate = LocalDate.parse(departureDate);

                YearMonth yearMonth = YearMonth.from(initialDate);
                LocalDate currentDate = LocalDate.of(yearMonth.getYear(), yearMonth.getMonth(), initialDate.getDayOfMonth());

                if (finalDate.getMonthValue() == initialDate.getMonthValue() && initialDate.getYear() == finalDate.getYear()) {
                    OccupancyValueModel valueModel = new OccupancyValueModel(new IntWritable(numNights), new IntWritable(totalRooms));
                    context.write(new OccupancyKeyModel(
                                    new Text(hotelCity),
                                    new Text(hotelId),
                                    new IntWritable(year),
                                    new IntWritable(month)),
                            valueModel);
                } else {

                    while (currentDate.getMonthValue() <= finalDate.getMonthValue() || yearMonth.getYear() < finalDate.getYear()) {
                        int numberNights = yearMonth.lengthOfMonth();

                        if (currentDate.getMonthValue() == initialDate.getMonthValue()  && yearMonth.getYear() == initialDate.getYear()) {
                            numberNights -= (initialDate.getDayOfMonth() - 1);
                        }

                        if (currentDate.getMonthValue() == finalDate.getMonthValue()  && yearMonth.getYear() == finalDate.getYear()) {
                            numberNights = finalDate.getDayOfMonth();
                        }

                        if(currentDate.getMonthValue() == finalDate.getMonthValue() && yearMonth.getYear() == finalDate.getYear()){
                            numberNights= numberNights - 1;
                        }

                        OccupancyValueModel valueModel = new OccupancyValueModel(new IntWritable(numberNights), new IntWritable(totalRooms));
                        context.write(new OccupancyKeyModel(
                                        new Text(hotelCity),
                                        new Text(hotelId),
                                        new IntWritable(yearMonth.getMonthValue()),
                                        new IntWritable(yearMonth.getYear())),
                                valueModel);


                        if(currentDate.getMonthValue() == 12){
                            break;
                        }

                        if (yearMonth.getMonthValue() == 12) {
                            yearMonth = yearMonth.plusYears(1);
                            yearMonth = yearMonth.withMonth(1);
                        } else {
                            yearMonth = yearMonth.plusMonths(1);
                        }
                        currentDate = LocalDate.of(yearMonth.getYear(), yearMonth.getMonth(), 1);

                    }
                }

            }
        }

    }

    public static class OccupancyReducer extends Reducer<OccupancyKeyModel, OccupancyValueModel, OccupancyKeyModel, Text> {

        public void reduce(OccupancyKeyModel key, Iterable<OccupancyValueModel> values, Context context) throws IOException, InterruptedException {
            int totalNumNights = 0;
            int totalRooms = 0;
            StringBuilder numberNights = new StringBuilder();

            List<OccupancyValueModel> list = new ArrayList<>();

            for (OccupancyValueModel value : values) {
                OccupancyValueModel occupancyValueModel = new OccupancyValueModel();
                occupancyValueModel.set(value);
                list.add(occupancyValueModel);
                totalRooms = value.getTotalRooms().get();
            }

            for (int  i = 0; i < list.size() ; i++) {
                totalNumNights += list.get(i).getTotalNights().get();
                numberNights.append(list.get(i).getTotalNights().get() + ",");
            }

            float nights = getDaysInMonth(key.getYear().get(), key.getMonth().get()) * totalRooms;

            float occupancyRate = ((float) totalNumNights / nights) * 100.0f;

            context.write(key, new Text(occupancyRate + ""));
        }

        public  int getDaysInMonth(int year, int month) {
            Calendar calendar = Calendar.getInstance();
            calendar.set(year, month, 1);
            int daysInMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
            return daysInMonth;
        }

    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Occupancy Rate");
        job.setJarByClass(AverageOccupancyPerHotelAndPerMonth.class);

        job.setMapperClass(OccupancyMapper.class);
        job.setReducerClass(OccupancyReducer.class);

        job.setOutputKeyClass(OccupancyKeyModel.class);
        job.setOutputValueClass(OccupancyValueModel.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
