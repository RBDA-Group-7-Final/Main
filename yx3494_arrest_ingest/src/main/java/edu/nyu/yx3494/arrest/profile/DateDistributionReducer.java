package edu.nyu.yx3494.arrest.profile;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class DateDistributionReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v : values){
            count += v.get();
        }
        context.write(key, new Text(count + " cases"));
    }
}
