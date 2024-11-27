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


public class GenericTop3PerCountry {

    public static class GenericTop3PerCountryMapper extends Mapper<Object, Text, Text, Text> {
        private Text country = new Text();
        private Text genericField = new Text();

        private int index;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            index = config.getInt("index", 0);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            country.set(fields[4]);
            genericField.set(fields[index]);
            context.write(country, genericField);
        }
    }

    public static class GenericTop3PerCountryReducer extends Reducer<Text, Text, Text, Text> {
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

    public static void main(String[] args) throws Exception
    {
        if (args.length < 3) {
            System.err.println("Usage: StarsByCountry <input_path> <output_path> <index> ");
            System.exit(1);
        }

        Configuration conf1 = new Configuration();
        conf1.setInt("index", Integer.parseInt(args[2]));

        Job job1 = Job.getInstance(conf1, "Generic Top 3 Per Country");
        job1.setJarByClass(GenericTop3PerCountry.class);
        job1.setMapperClass(GenericTop3PerCountry.GenericTop3PerCountryMapper.class);
        job1.setReducerClass(GenericTop3PerCountry.GenericTop3PerCountryReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }

}
