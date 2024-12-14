package edu.nyu.yx3494.arrest.profile;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class RaceDistributionReducer extends Reducer<Text, IntWritable, Text, Text> {
    Map<String, Integer> categoryDistribution = new HashMap<>();
    Pair<String, Integer> max = new Pair<>("", 0);
    Pair<String, Integer> min = new Pair<>("", Integer.MAX_VALUE);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
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
        // Update the category distribution
        categoryDistribution.put(key.toString(), count);
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("Total Race types"), new Text(String.valueOf(categoryDistribution.size())));
        context.write(new Text("Most Frequent Race"), new Text(max.getKey() + " (" + max.getValue() + " records)"));
        context.write(new Text("Least Frequent Race"), new Text(min.getKey() + " (" + min.getValue() + " records)"));

        // divider
        context.write(new Text("================== All Race =================="), new Text(""));
        // output all category distribution
        for (Map.Entry<String, Integer> entry : categoryDistribution.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue())));
        }
        super.cleanup(context);
    }
}
