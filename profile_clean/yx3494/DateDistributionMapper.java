package edu.nyu.yx3494.arrest.profile;

import edu.nyu.yx3494.ArrestRowParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DateDistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try{
            String[] fields = ArrestRowParser.parse(value.toString());
            if(fields[0].equals("ARREST_KEY")){
                return; // skip the header
            }
            String[] date = fields[1].split("/");
            String year = date[2];
            context.write(new Text(year), new IntWritable(1));
        } catch (RuntimeException e) {
            // drop invalid records
            context.write(new Text("Invalid"), new IntWritable(1));
        }
    }
}
