package edu.nyu.yx3494.arrest.profile;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class AgeDistributionReducer extends Reducer<Text, IntWritable, Text, Text> {
    Map<String, Integer> ageDistribution = new HashMap<>();
    int total = 0;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int count = 0;
        for (IntWritable v : values){
            count += v.get();
        }
        total += count;
        ageDistribution.put(key.toString(), count);
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : ageDistribution.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue()) + "(" + String.format("%.0f", (float)entry.getValue() / total * 100) + "%)"));
        }
        super.cleanup(context);
    }
}
