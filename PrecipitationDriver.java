import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrecipitationDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1: Aggregate precipitation by month/year
        Job job1 = Job.getInstance(conf, "Monthly Precipitation Sum");
        job1.setJarByClass(PrecipitationDriver.class);
        job1.setMapperClass(PrecipitationMapper.class);
        job1.setReducerClass(PrecipitationReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // Job 2: Find max precipitation month/year
        Job job2 = Job.getInstance(conf, "Max Precipitation Finder");
        job2.setJarByClass(PrecipitationDriver.class);
        job2.setMapperClass(MaxMapper.class);
        job2.setReducerClass(MaxReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}