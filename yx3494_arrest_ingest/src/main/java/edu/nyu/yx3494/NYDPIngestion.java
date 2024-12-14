package edu.nyu.yx3494;

import edu.nyu.yx3494.arrest.ArrestRecordWritable;
import edu.nyu.yx3494.arrest.clean.ArrestCleanMapper;
import edu.nyu.yx3494.arrest.clean.ArrestCleanReducer;
import edu.nyu.yx3494.arrest.profile.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class NYDPIngestion {
    final String cleanedArrestsPath = "/arrests_cleaned";
    final String arrestsBoroughProfilePath = "/arrests_borough_profile";
    final String arrestsCategoryProfilePath = "/arrests_category_profile";
    final String arrestsAgeProfilePath = "/arrests_age_profile";
    final String arrestsSexProfilePath = "/arrests_sex_profile";
    final String arrestsYearProfilePath = "/arrests_year_profile";
    final String arrestsOffenseProfilePath = "/arrests_offense_profile";
    final String arrestsTagProfilePath = "/arrests_tag_profile";
    final String arrestsRaceProfilePath = "/arrests_race_profile";
    final String arrestsCoordProfilePath = "/arrests_coord_profile";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 2) {
            System.err.println("Usage: NYDPIngestion <arrest file> <output path> [-p]");
            System.err.println("[-p] profile the data");
            System.exit(-1);
        }

        String arrestFilePath = args[0];
        String outputFile = args[1];
        NYDPIngestion nydpIngestion = new NYDPIngestion();
        boolean profile = Arrays.asList(args).contains("-p");
        if (profile){
            System.out.println("=== Profile the data ===");
            // verify the file
            nydpIngestion.launchFileVerifyProfileJob(arrestFilePath, outputFile);

            // profile the year
            nydpIngestion.launchYearProfileJob(arrestFilePath, outputFile);

            // profile the borough
            nydpIngestion.launchArrestBoroughProfileJob(arrestFilePath, outputFile);

            // profile the category
            nydpIngestion.launchCategoryProfileJob(arrestFilePath, outputFile);

            // profile the age
            nydpIngestion.launchAgeProfileJob(arrestFilePath, outputFile);

            // profile the tag
            nydpIngestion.launchTageProfileJob(arrestFilePath, outputFile);

            // profile the sex
            nydpIngestion.launchSexProfileJob(arrestFilePath, outputFile);

            // profile the race
            nydpIngestion.launchRaceProfileJob(arrestFilePath, outputFile);

            // profile the offense
            nydpIngestion.launchOffenseProfileJob(arrestFilePath, outputFile);

            // profile the coordinate
            nydpIngestion.launchCoordProfileJob(arrestFilePath, outputFile);
        }else{
            // clean the data
            System.out.println("=== Clean the data ===");
            nydpIngestion.launchArrestCleanJob(arrestFilePath, outputFile);
        }
    }

    /* =================== Clean Jobs =================== */
    void launchArrestCleanJob(String arrestFile, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest Cleaner");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + cleanedArrestsPath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(ArrestCleanMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(ArrestRecordWritable.class);
        job.setReducerClass(ArrestCleanReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    /* =================== Profile Jobs =================== */
    void launchFileVerifyProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Data Verify");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/verify_result"));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(VerifyMapper.class);
        job.setMapOutputKeyClass(BooleanWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(VerifyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    void launchArrestBoroughProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest Borough Distribution");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + arrestsBoroughProfilePath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(BoroughDistributionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(BoroughDistributionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    void launchCategoryProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest Category Distribution");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + arrestsCategoryProfilePath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(CategoryDistributionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(CategoryDistributionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    void launchCoordProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest Coord Distribution");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + arrestsCoordProfilePath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(CoordinateDistributionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(CoordinateDistributionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    void launchOffenseProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest Offense Distribution");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + arrestsOffenseProfilePath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(OffenseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(OffenseDistributionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    void launchYearProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest YEAR Distribution");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + arrestsYearProfilePath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(DateDistributionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(DateDistributionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    void launchRaceProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest Race Distribution");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + arrestsRaceProfilePath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(RaceDistributionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(RaceDistributionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    void launchTageProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest Tags Distribution");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + arrestsTagProfilePath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(TagDistributeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(TagDistributeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    void launchAgeProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest Age Distribution");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + arrestsAgeProfilePath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(AgeDistributionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(AgeDistributionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    void launchSexProfileJob(String arrestFile,String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJobName("NYPD Arrest Sex Distribution");
        FileInputFormat.addInputPath(job, new Path(arrestFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + arrestsSexProfilePath));
        job.setJarByClass(NYDPIngestion.class);
        job.setMapperClass(SexDistributionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(SexDistributionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }
}