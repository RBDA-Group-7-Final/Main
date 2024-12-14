package edu.nyu.yx3494.arrest.clean;

import edu.nyu.yx3494.ArrestRowParser;
import edu.nyu.yx3494.arrest.ArrestRecordWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ArrestCleanMapper extends Mapper<LongWritable, Text, NullWritable, ArrestRecordWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = ArrestRowParser.parse(value.toString());
        if(fields[0].equals("ARREST_KEY")){
            return; // skip the header
        }

        // field[3] PD_DESC and field[5] OFNS_DESC
        if (fields[3].contains("null") && fields[5].contains("null")) return;
        if (fields[3].isBlank() && fields[5].isBlank()) return;

        // field[7] ARREST_BORO
        if (fields[7].isBlank()) return;

        // field[11] AGE_GROUP
        if (fields[11].isBlank()) return;

        // field[14] X_COORD_CD, field[15] Y_COORD_CD
        if (fields[14].isBlank() || fields[15].isBlank()) return;

        ArrestRecordWritable record = new ArrestRecordWritable(fields);
        context.write(NullWritable.get(), record);
    }
}
