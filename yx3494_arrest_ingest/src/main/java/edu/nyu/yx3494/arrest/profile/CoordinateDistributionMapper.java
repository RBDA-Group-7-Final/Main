package edu.nyu.yx3494.arrest.profile;

import edu.nyu.yx3494.ArrestRowParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CoordinateDistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try{
            String[] fields = ArrestRowParser.parse(value.toString());
            if(fields[0].equals("ARREST_KEY")){
                return; // skip the header
            }
            if(!fields[14].isBlank() && !fields[15].isBlank()){
                Long.parseLong(fields[14]);
                Long.parseLong(fields[15]);
                context.write(new Text("Empty"), new IntWritable(1));
            } else {
                context.write(new Text("Valid"), new IntWritable(1));
            }
        } catch (Exception e) {
            // drop invalid records
            context.write(new Text("Invalid"), new IntWritable(1));
        }
    }
}
