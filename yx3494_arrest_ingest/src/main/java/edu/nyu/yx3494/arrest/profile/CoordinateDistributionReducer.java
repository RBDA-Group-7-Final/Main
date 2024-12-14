package edu.nyu.yx3494.arrest.profile;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class CoordinateDistributionReducer extends Reducer<Text, IntWritable, Text, Text> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        for (IntWritable v : values){
            counter += v.get();
        }
        if (counter > 1) {
            context.write(key, new Text(counter + " cases"));
        }
    }
}
