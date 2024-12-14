package edu.nyu.yx3494.arrest.profile;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SexDistributionReducer extends Reducer<Text, IntWritable, Text, Text> {
    int total = 0, female = 0, male = 0, unknown = 0;


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v : values) {
            count += v.get();
        }
        total += count;
        if (key.toString().equalsIgnoreCase("F")) {
            female += count;
        } else if (key.toString().equalsIgnoreCase("M")) {
            male += count;
        } else {
            unknown += count;
        }
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("Total Perpetrator"), new Text(String.valueOf(total)));
        context.write(new Text("Male Perpetrator"), new Text(male + " (" + String.format("%.0f", (double)male / total * 100) + "%)"));
        context.write(new Text("Female Perpetrator"), new Text(female + " (" + String.format("%.0f", (double)female / total * 100) + "%)"));
        context.write(new Text("Unknown Perpetrator"), new Text(unknown + " (" + String.format("%.0f", (double)unknown / total * 100) + "%)"));
        super.cleanup(context);
    }
}
