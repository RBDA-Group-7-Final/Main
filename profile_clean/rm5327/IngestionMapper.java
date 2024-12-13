import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.TimeUnit;


public class IngestionMapper
        extends Mapper<LongWritable, Text, Text, ComplaintStatsWritable> {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat anotherDateFormat = new SimpleDateFormat("MM/dd/yyyy");
    private SimpleDateFormat dateTimeFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa");

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (key.get() == 0 && value.toString().contains("As Of Date")) {
            return;
        }

        String[] fields = value.toString().split(",");
        if (fields.length != 14) {
            context.getCounter("Data Quality", "Invalid Record Length").increment(1);
            return;
        }

        try {
            String complaintID = fields[1].trim();

            Date incidentDate = dateFormat.parse(fields[2]);
            String incidentHour = fields[3].trim();

            Date receivedDate = dateTimeFormat.parse(fields[4]);
            Date closeDate = dateTimeFormat.parse(fields[5]);

            int hour = Integer.parseInt(incidentHour);
            if (hour < 0 || hour > 23) {
                context.getCounter("Data Quality", "Invalid Hour").increment(1);
                incidentHour = "UNKNOWN";
            }

            String borough = fields[6].trim().toUpperCase();
            if (borough.isEmpty()) {
                context.getCounter("Data Quality", "Missing Borough").increment(1);
                borough = "UNKNOWN";
            }

            String locationType = fields[8].trim();
            if (locationType.isEmpty()) {
                context.getCounter("Data Quality", "Missing Location Type").increment(1);
                locationType = "UNKNOWN";
            }

            String reason = fields[9].trim();
            if (reason.isEmpty()) {
                context.getCounter("Data Quality", "Missing Police Contact Reason").increment(1);
                reason = "UNKNOWN";
            }

            String outcome = fields[10].trim();
            if (outcome.isEmpty()) {
                context.getCounter("Data Quality", "Missing Outcome").increment(1);
                outcome = "UNKNOWN";
            }

            long reportingDelay = receivedDate.getTime() - incidentDate.getTime();
            if (reportingDelay < 0) {
                context.getCounter("Data Quality", "Invalid Date Sequence").increment(1);
            }

            String dispotion = fields[11].trim();
            if (dispotion.isEmpty()) {
                context.getCounter("Data Quality", "Missing Dispotion").increment(1);
                dispotion = "UNKNOWN";
            }

            ComplaintStatsWritable stats = new ComplaintStatsWritable(
                complaintID, borough, locationType, reason, outcome, dispotion,
                incidentHour, anotherDateFormat.format(incidentDate) + " " + incidentHour,
                dateTimeFormat.format(receivedDate), dateTimeFormat.format(closeDate)
            );

            context.write(new Text(complaintID), stats);

        } catch (ParseException e) {
            context.getCounter("Data Quality", "Invalid Date Format").increment(1);
        } catch (NumberFormatException e) {
            context.getCounter("Data Quality", "Invalid Hour Format").increment(1);
        }
    }
}
