package edu.nyu.yx3494.arrest.clean;

import edu.nyu.yx3494.arrest.ArrestRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ArrestCleanReducer extends Reducer<NullWritable, ArrestRecordWritable, NullWritable, Text> {
    // variable to check if header is already written
    boolean outputHeader = false;
    public void reduce(NullWritable key, Iterable<ArrestRecordWritable> values, Context context) throws IOException, InterruptedException {
        // write the header at the beginning
        if (!outputHeader) {
            context.write(NullWritable.get(), ArrestRecordWritable.header());
            outputHeader = true;
        }

        // output the cleaned records
        for (ArrestRecordWritable v : values){
            context.write(NullWritable.get(), v.outputText());
        }
    }
}
