package it.unipi.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InMemoryMovingAverage {

    public static class ParseMapper extends Mapper<Object, Text, Text, TimeSeriesData> {

        // reuse Hadoop's Writable objects
        private final Text reducerKey = new Text();
        private final TimeSeriesData reducerValue = new TimeSeriesData();
       
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
            String valueStr = value.toString();
            String[] split = valueStr.trim().split(",");

            if(split.length != 3) {
                System.out.println("Wrong format: " + valueStr);
                return;
            }

            
            long timestamp = DateUtil.getLongFromStringDate(split[1]);

            if(timestamp == -1) {
                System.out.println("Wrong format of the date: " + split[0]);
                return;
            }

            double price = Double.parseDouble(split[2]);

            reducerKey.set(split[0]);
            reducerValue.set(timestamp, price);

            System.out.println("Key value pair: " + reducerKey + ", " + reducerValue.toString());
            context.write(reducerKey, reducerValue);
        }
    }

    public static class SortAverageReducer extends Reducer<Text,TimeSeriesData, Text, Text >{

        private int windowSize;

        public void setup(Context context) {
            windowSize = context.getConfiguration().getInt("moving.average.window.size", 5);
        }

        public void reduce(Text key, Iterable<TimeSeriesData> values, Context context) 
          throws IOException, InterruptedException {
            List<TimeSeriesData> list = new ArrayList<TimeSeriesData>();
            for (TimeSeriesData val : values) {
                list.add(TimeSeriesData.copy(val));
            }
            
            Collections.sort(list); 
            
            double sum = 0;
            double average = 0;
            Text outputval = new Text();

            for (int i = 0; i < windowSize - 1; i++)
               sum += list.get(i).getAverage();

            for(int i = windowSize - 1; i < list.size(); i++){
                sum += list.get(i).getAverage();
                average = sum / windowSize;

                String date = DateUtil.getStringFromLong(list.get(i).getTimestamp());
                outputval.set(date +", " + average);
                context.write(key, outputval); 
                sum -= list.get(i - windowSize + 1).getAverage();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3){
            System.err.println("Usage: movingaverage <window_size> <in> <out>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "moving average");
        job.setJarByClass(InMemoryMovingAverage.class);

        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(SortAverageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TimeSeriesData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setInt("moving.average.window.size", Integer.parseInt(otherArgs[0]));

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}