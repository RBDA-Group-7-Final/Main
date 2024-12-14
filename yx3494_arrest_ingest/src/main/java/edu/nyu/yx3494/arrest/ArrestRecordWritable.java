package edu.nyu.yx3494.arrest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArrestRecordWritable implements WritableComparable<ArrestRecordWritable> {
    // the arrest data in format of "MM/DD/YYYY"
    public String arrestDate;
    //month, year
    public int month, year;
    // details of the arrest, this is additional information for the category
    public String tags;
    // classification group description (Category)
    public String category;
    // offense level
    public String offenseLevel; // felony, misdemeanor, violation
    // arrest borough
    public String arrestBorough;
    // age group
    public String ageGroup;
    // perpetrator sex
    public String perpSex;
    // perpetrator race
    public String perpRace;
    // x coordinate and y coordinate
    public long xCoordinate, yCoordinate;
    // latitude and longitude
    public double latitude, longitude;
    // default constructor
    public ArrestRecordWritable() {
    }

    public ArrestRecordWritable(String[] fields) {
        this.arrestDate = fields[1];
        // format of the date is "MM/DD/YYYY"
        String[] date = arrestDate.split("/");
        this.month = Integer.parseInt(date[0]);
        this.year = Integer.parseInt(date[2]);
        this.tags = fields[3];
        this.category = fields[5];
        this.offenseLevel = getOffenseLevel(fields[7]);
        this.arrestBorough = getArrestBorough(fields[8]);
        this.ageGroup = getAgeGroup(fields[11]);
        this.perpSex = getPerpSex(fields[12]);
        this.perpRace = fields[13].toLowerCase();

        // some value have decimal point
        this.xCoordinate = (long) Double.parseDouble(fields[14]);
        this.yCoordinate = (long) Double.parseDouble(fields[15]);
        this.latitude = Double.parseDouble(fields[16]);
        this.longitude = Double.parseDouble(fields[17]);
    }

    public Text outputText() {
        return new Text(
                arrestDate + "," +
                        month + "," +
                        year + ",\"" +
                        tags  + "\",\"" +
                        category + "\"," +
                        offenseLevel + "," +
                        arrestBorough + "," +
                        ageGroup + "," +
                        perpSex + "," +
                        perpRace + "," +
                        xCoordinate + "," +
                        yCoordinate + "," +
                        latitude + "," +
                        longitude
        );
    }

    /**
     * Convert offense level to readable format
     */
    private String getOffenseLevel(String offenseLevel) {
        if (offenseLevel.equalsIgnoreCase("F")) {
            return "felony";
        } else if (offenseLevel.equalsIgnoreCase("M")) {
            return "misdemeanor";
        } else if (offenseLevel.equalsIgnoreCase("V")) {
            return "violation";
        } else {
            return "unknown";
        }
    }

    /**
     * Convert arrest borough to readable format
     */
    private String getArrestBorough(String arrestBorough) {
        if (arrestBorough.equalsIgnoreCase("B")) {
            return "Bronx";
        } else if (arrestBorough.equalsIgnoreCase("K")) {
            return "Brooklyn";
        } else if (arrestBorough.equalsIgnoreCase("M")) {
            return "Manhattan";
        } else if (arrestBorough.equalsIgnoreCase("Q")) {
            return "Queens";
        } else if (arrestBorough.equalsIgnoreCase("S")) {
            return "Staten Island";
        } else {
            return "unknown";
        }
    }

    /**
     * Convert perpetrator sex to readable format
     */
    private String getPerpSex(String perpSex) {
        if (perpSex.equalsIgnoreCase("M")) {
            return "male";
        } else if (perpSex.equalsIgnoreCase("F")) {
            return "female";
        } else {
            return "unknown";
        }
    }

    /**
     * Unify age group format and avoid any exceptions
     */
    private String getAgeGroup(String ageGroup) {
        try {
            int age = Integer.parseInt(ageGroup);
            if (age < 18) {
                return "<18";
            } else if (age < 25) {
                return "18-24";
            } else if (age < 45) {
                return "25-44";
            } else if (age < 65) {
                return "45-64";
            } else {
                return "65+";
            }
        } catch (NumberFormatException e) {
            return ageGroup;
        }
    }

    static public Text header() {
        String sb = "arrest_date," +
                "month," +
                "year," +
                "tags," +
                "category," +
                "offense_level," +
                "borough," +
                "age_group," +
                "sex," +
                "race," +
                "x_coord," +
                "y_coord" +
                "latitude" +
                "longitude";
        return new Text(sb);
    }

    @Override
    public int compareTo(ArrestRecordWritable o) {
        return this.arrestDate.compareTo(o.arrestDate);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(arrestDate);
        dataOutput.writeInt(month);
        dataOutput.writeInt(year);
        dataOutput.writeUTF(tags);
        dataOutput.writeUTF(category);
        dataOutput.writeUTF(offenseLevel);
        dataOutput.writeUTF(arrestBorough);
        dataOutput.writeUTF(ageGroup);
        dataOutput.writeUTF(perpSex);
        dataOutput.writeUTF(perpRace);
        dataOutput.writeLong(xCoordinate);
        dataOutput.writeLong(yCoordinate);
        dataOutput.writeDouble(latitude);
        dataOutput.writeDouble(longitude);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        arrestDate = dataInput.readUTF();
        month = dataInput.readInt();
        year = dataInput.readInt();
        tags = dataInput.readUTF();
        category = dataInput.readUTF();
        offenseLevel = dataInput.readUTF();
        arrestBorough = dataInput.readUTF();
        ageGroup = dataInput.readUTF();
        perpSex = dataInput.readUTF();
        perpRace = dataInput.readUTF();
        xCoordinate = dataInput.readLong();
        yCoordinate = dataInput.readLong();
        latitude = dataInput.readDouble();
        longitude = dataInput.readDouble();
    }
}
