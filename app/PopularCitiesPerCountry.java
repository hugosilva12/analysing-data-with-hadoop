package tead;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.*;


public class PopularCitiesPerCountry {


    public static class PopularCitiesMapper extends Mapper<Object, Text, Text, Text> {
        private Text country = new Text();
        private Text city = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String reservationStatus = fields[5];

            if (reservationStatus.equals("Registado") || reservationStatus.equals("Confirmado")) {
                country.set(fields[4]);
                city.set(fields[0]);
                context.write(country, city);
            }
        }
    }

    public static class PopularCitiesReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> cityCountMap = new HashMap<>();

            for (Text city : values) {
                String cityStr = city.toString();
                cityCountMap.merge(cityStr, 1, Integer::sum);
            }

            List<Map.Entry<String, Integer>> cityCountList = new ArrayList<>(cityCountMap.entrySet());

            Collections.sort(cityCountList, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });


            Map<String, Integer> sortedCityCountMap = new LinkedHashMap<>();
            int count = 0;
            for (Map.Entry<String, Integer> entry : cityCountList) {
                sortedCityCountMap.put(entry.getKey(), entry.getValue());
                count++;
                if (count == 3) {
                    break;
                }
            }

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : sortedCityCountMap.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
            }

            String result = sb.toString();
            if (!result.isEmpty()) {
                result = result.substring(0, result.length() - 2);
            }

            StringBuilder keyFormatted = new StringBuilder();
            keyFormatted.append(key.toString()).append(" ").append("--");
            Text concatenatedKey = new Text(keyFormatted.toString());

            context.write(concatenatedKey, new Text(result));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular cities by country");
        job.setJarByClass(PopularCitiesPerCountry.class);
        job.setMapperClass(PopularCitiesMapper.class);
        job.setReducerClass(PopularCitiesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class AverageOccupancyCityMonth {

        public static class OccupancyMapper extends Mapper<Object, Object, OccupancyKeyModel, OccupancyValueModel> {

            public void map(Object key, Object value, Context context) throws IOException, InterruptedException {

                String[] fields = value.toString().split(",");

                String reservationStatus = fields[5];

                if (reservationStatus.equals("Registado") || reservationStatus.equals("Confirmado")) {
                    String hotelCity = fields[0];
                    String hotelId = fields[1];
                    String reservationDate = fields[9];
                    int numNights = Integer.parseInt(fields[11]);
                    int totalRooms = Integer.parseInt(fields[29]);

                    String[] dateParts = reservationDate.toString().split("-");
                    int year = Integer.parseInt(dateParts[0]);
                    int month = Integer.parseInt(dateParts[1]);

                    OccupancyValueModel valueModel = new OccupancyValueModel(new IntWritable(numNights), new IntWritable(totalRooms));

                    context.write(new OccupancyKeyModel(
                                    new Text(hotelCity),
                                    new Text(hotelId),
                                    new IntWritable(year),
                                    new IntWritable(month)),
                            valueModel);

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
            job.setJarByClass(AverageOccupancyCityMonth.class);

            job.setMapperClass(OccupancyMapper.class);
            job.setReducerClass(OccupancyReducer.class);

            job.setOutputKeyClass(OccupancyKeyModel.class);
            job.setOutputValueClass(OccupancyValueModel.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
