import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.util.concurrent.TimeUnit;


public class IngestionReducer
        extends Reducer<Text, ComplaintStatsWritable, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<ComplaintStatsWritable> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder output = new StringBuilder();

        for (ComplaintStatsWritable stats : values) {
            output.append(stats.getIncidentTimestamp().toString());
            output.append(",");
            output.append(stats.getReceivedTimestamp().toString());
            output.append(",");
            output.append(stats.getCloseTimestamp().toString());
            output.append(",");
            output.append(stats.getBorough().toString());
            output.append(",");
            output.append(stats.getLocationType().toString());
            output.append(",");
            output.append(stats.getPoliceContactReason().toString());
            output.append(",");
            output.append(stats.getOutcome().toString());
            output.append(",");
            output.append(stats.getDispotion().toString());
        }

        context.write(NullWritable.get(), new Text(output.toString()));
    }
}
