import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// Mapper for Max Finder
class MaxMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), value);
    }
}

// Reducer for Max Finder
public class MaxPrecipitationReducer extends Reducer<NullWritable, Text, Text, FloatWritable> {
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float max = Float.MIN_VALUE;
        String maxMonthYear = "";

        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            String monthYear = parts[0];
            float total = Float.parseFloat(parts[1]);

            if (total > max) {
                max = total;
                maxMonthYear = monthYear;
            }
        }

        // Format output nicely
        String[] parts = maxMonthYear.split("/");
        String month = parts[0];
        String year = parts[1];
        context.write(new Text(month + "th month in " + year + " had the highest total precipitation"), new FloatWritable(max));
    }
}
