package edu.nyu.yx3494.arrest.profile;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class VerifyReducer extends Reducer<BooleanWritable, Text, Text, Text> {
    @Override
    protected void reduce(BooleanWritable key, Iterable<Text> values, Reducer<BooleanWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Text v : values) {
            count ++;
            if (!key.get()) {
                context.write(new Text("Invalided Row"), new Text(v.toString()));
            }
        }
        if (key.get()) {
            context.write(new Text("Verified Row"), new Text(String.valueOf(count)));
        } else {
            context.write(new Text("Invalided Row"), new Text(String.valueOf(count)));
        }
    }
}
