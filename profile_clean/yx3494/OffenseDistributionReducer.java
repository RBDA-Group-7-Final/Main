package edu.nyu.yx3494.arrest.profile;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class OffenseDistributionReducer extends Reducer<Text, IntWritable, Text, Text> {
    int total, felony, misdemeanor, violation;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v : values){
            count += v.get();
        }
        total += count;
        if (key.toString().equalsIgnoreCase("F")) {
            felony += count;
        } else if (key.toString().equalsIgnoreCase("M")) {
            misdemeanor += count;
        } else if (key.toString().equalsIgnoreCase("V")) {
            violation += count;
        }
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("Felony"), new Text(felony + String.format(" (%.0f%%)", (float)felony / total * 100)));
        context.write(new Text("Misdemeanor"), new Text(misdemeanor + String.format(" (%.0f%%)", (float)misdemeanor / total * 100)));
        context.write(new Text("Violation"), new Text(violation + String.format(" (%.0f%%)", (float)violation / total * 100)));

        super.cleanup(context);
    }
}
