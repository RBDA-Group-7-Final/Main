package edu.nyu.yx3494.arrest.profile;

import edu.nyu.yx3494.ArrestRowParser;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VerifyMapper extends Mapper<LongWritable, Text, BooleanWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            ArrestRowParser.parse(value.toString());
            context.write(new BooleanWritable(true), new Text(""));
        } catch (RuntimeException e) {
            context.write(new BooleanWritable(false), value);
        }
    }
}
