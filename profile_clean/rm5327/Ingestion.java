import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


class ComplaintStatsWritable implements WritableComparable<ComplaintStatsWritable> {
    private Text complaintID;
    private Text borough;
    private Text locationType;
    private Text policeContactReason;
    private Text outcome;
    private Text dispotion;
    private Text incidentHour;
    private Text incidentTimestamp;
    private Text receivedTimestamp;
    private Text closeTimestamp;

    public ComplaintStatsWritable() {
        this.complaintID = new Text();
        this.borough = new Text();
        this.locationType = new Text();
        this.policeContactReason = new Text();
        this.outcome = new Text();
        this.dispotion = new Text();
        this.incidentHour = new Text();
        this.incidentTimestamp = new Text();
        this.receivedTimestamp = new Text();
        this.closeTimestamp = new Text();
    }

    public ComplaintStatsWritable(String complaintID, String borough, String locationType, String reason, String outcome, String dispotion,
            String incidentHour, String incidentTs, String receivedTs, String closeTs)
        {
        this.complaintID = new Text(complaintID);
        this.borough = new Text(borough);
        this.locationType = new Text(locationType);
        this.policeContactReason = new Text(reason);
        this.outcome = new Text(outcome);
        this.dispotion = new Text(dispotion);
        this.incidentHour = new Text(incidentHour);
        this.incidentTimestamp = new Text(incidentTs);
        this.receivedTimestamp = new Text(receivedTs);
        this.closeTimestamp = new Text(closeTs);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        complaintID.write(out);
        borough.write(out);
        locationType.write(out);
        policeContactReason.write(out);
        outcome.write(out);
        dispotion.write(out);
        incidentHour.write(out);
        incidentTimestamp.write(out);
        receivedTimestamp.write(out);
        closeTimestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        complaintID.readFields(in);
        borough.readFields(in);
        locationType.readFields(in);
        policeContactReason.readFields(in);
        outcome.readFields(in);
        dispotion.readFields(in);
        incidentHour.readFields(in);
        incidentTimestamp.readFields(in);
        receivedTimestamp.readFields(in);
        closeTimestamp.readFields(in);
    }

    @Override
    public int compareTo(ComplaintStatsWritable o) {
        return this.incidentTimestamp.compareTo(o.incidentTimestamp);
    }

    public Text getComplaintID() { return complaintID; }
    public Text getBorough() { return borough; }
    public Text getLocationType() { return locationType; }
    public Text getPoliceContactReason() { return policeContactReason; }
    public Text getOutcome() { return outcome; }
    public Text getDispotion() { return dispotion; }
    public Text getIncidentHour() { return incidentHour; }
    public Text getIncidentTimestamp() { return incidentTimestamp; }
    public Text getReceivedTimestamp() { return receivedTimestamp; }
    public Text getCloseTimestamp() { return closeTimestamp; }
}

public class Ingestion {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: Ingestion <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(Ingestion.class);
        job.setJobName("Data Ingestion");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(IngestionMapper.class);
        job.setReducerClass(IngestionReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(ComplaintStatsWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}