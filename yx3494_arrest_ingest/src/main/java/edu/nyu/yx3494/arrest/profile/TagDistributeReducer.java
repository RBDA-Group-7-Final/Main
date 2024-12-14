package edu.nyu.yx3494.arrest.profile;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class TagDistributeReducer extends Reducer<Text, IntWritable, Text, Text> {
    int validCount = 0, invalidCount = 0, totalCount = 0;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v : values){
            count += v.get();
        }
        if (key.toString().equalsIgnoreCase("") || key.toString().equalsIgnoreCase("null")) {
            invalidCount += count;
        } else {
            validCount += count;
        }
        totalCount += count;
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("Total Record"), new Text(String.valueOf(totalCount)));
        context.write(new Text("Invalid"), new Text(invalidCount  + " records"));
        context.write(new Text("Valid"), new Text(validCount + " records"));
    }
}
