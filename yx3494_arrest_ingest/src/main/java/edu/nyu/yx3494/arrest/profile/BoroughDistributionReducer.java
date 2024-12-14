package edu.nyu.yx3494.arrest.profile;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class BoroughDistributionReducer extends Reducer<Text, IntWritable, Text, Text> {
    Map<String, Integer> boroughDistribution = new HashMap<>();
    Pair<String, Integer> max = new Pair<>("", 0);
    Pair<String, Integer> min = new Pair<>("", Integer.MAX_VALUE);

    // Static the borough counter
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v : values){
            count += v.get();
        }
        // Update the max
        if (count > max.getSecond()) {
            max = new Pair<>(key.toString(), count);
        }
        // Update the min
        if (count < min.getSecond()) {
            min = new Pair<>(key.toString(), count);
        }
        // Update the borough distribution
        boroughDistribution.put(key.toString(), count);
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("Total Boroughs"), new Text(String.valueOf(boroughDistribution.size())));
        context.write(new Text("Most Frequent Boroughs"), new Text(max.getKey() + " (" + max.getValue() + " arrests)"));
        context.write(new Text("Least Frequent Boroughs"), new Text(min.getKey() + " (" + min.getValue() + " arrests)"));
        context.write(new Text("================== All =================="), new Text(""));

        // output all boroughs distribution
        for (Map.Entry<String, Integer> entry : boroughDistribution.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue())));
        }
        super.cleanup(context);
    }
}
